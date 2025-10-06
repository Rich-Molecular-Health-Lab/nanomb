#############################################
# Snakefile (container-first; CPU vs GPU split)
#############################################

import os, glob, csv, string
from pathlib import Path
from snakemake.exceptions import WorkflowError

configfile: "config/config.yaml"

# --- roots & helpers ---
PROJ    = os.environ.get("PROJ_ROOT", os.getcwd())
WORK    = os.environ.get("WORK",    os.path.join(PROJ, "local_work"))
NRDSTOR = os.environ.get("NRDSTOR", os.environ.get("NRDSTOR_LOCAL", ""))

def _expand(v):  return os.path.expandvars(v) if isinstance(v, str) else v
def R(rule, key, default): return config.get("resources", {}).get(rule, {}).get(key, default)

def fmt_template(s, **vals):
    if not isinstance(s, str): return s
    s = os.path.expandvars(s)
    tmpl = string.Template(s.replace("{", "${").replace("}", "}"))
    return tmpl.safe_substitute(**vals)

def resolve_path(base, path_template, **vals):
    p = fmt_template(path_template, **vals)
    if not p: return ""
    return p if os.path.isabs(p) else os.path.join(base, p)

# --- dataset & layout ---
SAMPLESET = _expand(config.get("sampleset", "")).strip()
DATASET   = _expand(config.get("dataset", "")).strip()
if not DATASET:
    raise WorkflowError("config.dataset is required")

OUT_ROOT  = _expand(config.get("out_root", None))
if not OUT_ROOT:
    raise WorkflowError("Config is missing 'out_root'.")

LAYOUT = { **{
    "dataset_dir":   "{sampleset}/{dataset}",
    "raw_dir":       "{sampleset}/{dataset}/raw",
    "pod5_dir":      "{sampleset}/pod5",
    "basecall_dir":  "{sampleset}/basecalled",
    "demux_dir":     "{sampleset}/demuxed",
    "summary_dir":   "{sampleset}/dorado_summaries",
    "sample_sheet_dir": "{sampleset}/samples",
    "sample_sheet_name": "{run}_sample_sheet.csv",
    "sample_routing": "{sampleset}/samples/sample_to_dataset.tsv",
}, **config.get("layout", {}) }

DSET = resolve_path(OUT_ROOT, LAYOUT["dataset_dir"], sampleset=SAMPLESET, dataset=DATASET)
OUT  = DSET
TMP  = os.path.join(DSET, "tmp")

READS_IN_CFG = _expand(config.get("reads_in","")).strip()
POD5_IN_CFG  = _expand(config.get("pod5_in","")).strip()
USE_POD5 = bool(POD5_IN_CFG)

POD5_IN = POD5_IN_CFG or resolve_path(OUT_ROOT, LAYOUT["pod5_dir"], sampleset=SAMPLESET, dataset=DATASET)
RAW     = READS_IN_CFG or resolve_path(OUT_ROOT, LAYOUT["raw_dir"], sampleset=SAMPLESET, dataset=DATASET)

BASECALL_DIR = resolve_path(OUT_ROOT, LAYOUT["basecall_dir"], sampleset=SAMPLESET, dataset=DATASET)
DEMUX_DIR    = resolve_path(OUT_ROOT, LAYOUT["demux_dir"],    sampleset=SAMPLESET, dataset=DATASET)
SUMMARY_DIR  = resolve_path(OUT_ROOT, LAYOUT["summary_dir"],  sampleset=SAMPLESET, dataset=DATASET)

SHEET_DIR  = resolve_path(OUT_ROOT, LAYOUT["sample_sheet_dir"],  sampleset=SAMPLESET, dataset=DATASET)
SHEET_NAME = LAYOUT["sample_sheet_name"]
ROUTING_TSV = resolve_path(OUT_ROOT, LAYOUT["sample_routing"], sampleset=SAMPLESET, dataset=DATASET)

# References
ITGDB_UDB   = _expand(config.get("itgdb_udb", ""))
SILVA_FASTA = _expand(config.get("silva_fasta", ""))

# Tunables
MAP_ID  = float(config.get("map_id", 0.98))
STRAND  = config.get("strand", "both")
THREADS = int(config.get("threads", 16))
NANOASV = config.get("nanoasv_bin", "nanoasv")
SINTAX_CUTOFF = float(config.get("sintax_cutoff", 0.8))
MIN_UNIQUE = int(config.get("min_unique_size", 1))

# Containers
CONTAINERS = {
    "cpu":    _expand(config.get("container_cpu",    "docker://aliciamrich/nanomb:0.2.0-cpu")),
    "gpu":    _expand(config.get("container_gpu",    "docker://aliciamrich/nanombgpu:0.2.0-gpu")),
    "nanoasv":_expand(config.get("container_nanoasv","docker://aliciamrich/nanoasv:0.2.0-cpu")),
    # If you want a separate IQ-TREE image, set it; otherwise reuse CPU:
    "iqtree2":_expand(config.get("container_iqtree2", "")) or _expand(config.get("container_cpu", "docker://aliciarich/nanomb:0.2.0-cpu")),
}

# When starting from POD5, canonicalize RAW to DSET/raw
if USE_POD5:
    RAW = os.path.join(DSET, "raw")

# Polishing paths
POLISH_DIR   = os.path.join(TMP, "polished")
ALL_READS_FQ = os.path.join(POLISH_DIR, "all_reads.fastq")
MAP_BAM_99   = os.path.join(POLISH_DIR, "map_r0.bam")
R1_FASTA     = os.path.join(POLISH_DIR, "r1.fasta")
MAP_BAM_R1   = os.path.join(POLISH_DIR, "map_r1.bam")
R2_FASTA     = os.path.join(POLISH_DIR, "r2.fasta")
POLISHED     = os.path.join(POLISH_DIR, "polished_otus.fasta")

# Ensure base dirs
for p in (OUT, TMP, os.path.join(OUT,"otu"), os.path.join(OUT,"asv"), os.path.join(OUT,"logs"), os.path.join(OUT,"benchmarks")):
    Path(p).mkdir(parents=True, exist_ok=True)

def asv_target():
    m = config.get("asv_method", None)
    if m == "nanoasv": return os.path.join(OUT, "asv/nanoasv/phyloseq.RData")
    if m == "nanoclust": return os.path.join(OUT, "asv/nanoclust/results.done")
    if m == "dada2_ont": return os.path.join(OUT, "asv/dada2_ont/phyloseq.RData")
    return []

def demux_index_path(): return os.path.join(TMP, "demux_index.tsv")

def load_routing_for_dataset(dataset):
    """
    Return a dict: sample -> desired run (or None if unspecified).
    TSV columns: sample, dataset, [run]
    """
    picks = {}
    if not ROUTING_TSV or not Path(ROUTING_TSV).exists():
        return picks
    with open(ROUTING_TSV) as fh:
        rdr = csv.DictReader(fh, delimiter="\t")
        for row in rdr:
            if row.get("dataset","").strip() == dataset:
                s = row.get("sample","").strip()
                r = (row.get("run","") or "").strip() or None
                if s:
                    picks[s] = r
    return picks

# --------- Checkpoint the demux index (critical) ----------
checkpoint dorado_demux_index:
    input: demuxed = directory(DEMUX_DIR)
    output: tsv = demux_index_path()
    message: "Indexing demuxed BAMs -> {output.tsv}"
    shell:
        "python - <<'PY'\n"
        "from pathlib import Path\n"
        "import csv, os\n"
        f"PDEMUX=r'''{DEMUX_DIR}'''\n"
        f"OUT=r'''{demux_index_path()}'''\n"
        "Path(os.path.dirname(OUT)).mkdir(parents=True, exist_ok=True)\n"
        "rows=[]\n"
        "for run_dir in sorted(Path(PDEMUX).glob('*')):\n"
        "  if not run_dir.is_dir():\n"
        "    continue\n"
        "  run = run_dir.name\n"
        "  for bam in sorted(run_dir.rglob('*.bam')):\n"
        "    base = bam.stem\n"
        "    # If filenames look like '<run>_<sample>.bam', strip prefix; else use base\n"
        "    prefix = run + '_'\n"
        "    sample = base[len(prefix):] if base.startswith(prefix) else base\n"
        "    rows.append((sample, run, str(bam)))\n"
        "with open(OUT,'w',newline='') as fh:\n"
        "  w=csv.writer(fh, delimiter='\\t')\n"
        "  w.writerow(['sample','run','bam'])\n"
        "  w.writerows(rows)\n"
        "PY"
        
def _samples_from_ckpt(wc):
    idx = checkpoints.dorado_demux_index.get(**wc).output.tsv
    routing = load_routing_for_dataset(DATASET)
    samples = set()
    import csv as _csv
    with open(idx) as fh:
        rdr = _csv.DictReader(fh, delimiter="\t")
        for row in rdr:
            s, run = row["sample"], row["run"]
            wanted_run = routing.get(s, None)
            if s in routing and (wanted_run is None or wanted_run == run):
                samples.add(s)
    return sorted(samples)


def _fastqs_from_ckpt(wc):
    return expand(os.path.join(RAW, "{sample}.fastq"), sample=_samples_from_ckpt(wc))

def bam_for_sample(wc):
    idx = checkpoints.dorado_demux_index.get(**wc).output.tsv
    routing = load_routing_for_dataset(DATASET)
    wanted_run = routing.get(wc.sample, None)
    matches = []
    import csv as _csv
    with open(idx) as fh:
        rdr = _csv.DictReader(fh, delimiter="\t")
        for row in rdr:
            if row["sample"] == wc.sample and (wanted_run is None or row["run"] == wanted_run):
                matches.append(row)
    if not matches:
        raise WorkflowError(f"No demuxed BAM for sample={wc.sample!r} (wanted run={wanted_run}) in {idx}")
    # If multiple remain (no run specified), prefer lexicographically latest run
    pick = sorted(matches, key=lambda r: r["run"])[-1]
    return pick["bam"]
  
# ---------------- targets ----------------
rule all:
    input:
        os.path.join(TMP, "preflight.ok"),
        os.path.join(OUT, "manifest.txt"),
        os.path.join(OUT, "benchmarks/fastcat_filter.tsv"),
        os.path.join(OUT, "qc/nanoplot"),
        POLISHED,
        os.path.join(OUT, "otu/otu_table_merged.tsv"),
        os.path.join(OUT, "otu/otu_references_aligned.fasta"),
        os.path.join(OUT, "otu/otu_tree.treefile"),
        os.path.join(OUT, "otu/otus_taxonomy.sintax"),
        expand(os.path.join(OUT, "otu/otus_centroids{id}.fasta"), id=["_99","_97"]),
        asv_target()

rule preflight:
    input: db = ITGDB_UDB
    output: touch(os.path.join(TMP, "preflight.ok"))
    run:
        if not input.db or not os.path.exists(input.db) or os.path.getsize(input.db)==0:
            raise WorkflowError(f"SINTAX DB missing/empty: {input.db!r}")
        if USE_POD5:
            if not Path(POD5_IN).exists() or not any(Path(POD5_IN).glob("*")):
                raise WorkflowError(f"No POD5 runs found under {POD5_IN}")
        else:
            fq = glob.glob(os.path.join(RAW, "*.fastq")) + glob.glob(os.path.join(RAW, "*.fastq.gz"))
            if not fq:
                raise WorkflowError(f"No FASTQs found under RAW={RAW}")
        for k,img in CONTAINERS.items():
            if not img:
                raise WorkflowError(f"Container path/URI for '{k}' not set.")

rule manifest:
    output: os.path.join(OUT, "manifest.txt")
    run:
        import subprocess, yaml
        commit = subprocess.getoutput(f"git -C {PROJ} rev-parse --short HEAD")
        with open(output[0], "w") as fh:
            fh.write(f"commit: {commit}\n")
            fh.write(f"dataset: {DATASET}\n")
            for k in ["WORK","NRDSTOR","PROJ_ROOT"]:
                fh.write(f"{k}={os.environ.get(k,'')}\n")
            fh.write("\nconfig:\n")
            yaml.safe_dump(config, fh, sort_keys=False)

# ---------------- Dorado (GPU via explicit --nv) ----------------
rule dorado_basecall:
    input: pod5 = POD5_IN
    output:
        basecalled = directory(BASECALL_DIR),
        summaries  = directory(os.path.join(SUMMARY_DIR, "basecall"))
    threads: R("dorado_basecall","threads",8)
    resources:
        mem_mb   = R("dorado_basecall","mem_mb",32000),
        runtime  = R("dorado_basecall","runtime",180),
        partition= R("dorado_basecall","partition","gpu"),
        account  = R("dorado_basecall","account","richlab"),
        extra    = R("dorado_basecall","extra","--gres=gpu:1")
    params:
        modelname = lambda wc: config.get("dorado_model_name","sup"),
        modelsdir = lambda wc: _expand(config.get("dorado_models_dir","/models")),
        extra     = lambda wc: config.get("dorado_extra","")
    log: os.path.join(OUT, "logs/dorado_basecall.log")
    container: CONTAINERS["gpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p "{output.basecalled}" "{output.summaries}"
      shopt -s nullglob
      for d in "{input.pod5}"/*; do
        [[ -d "$d" ]] || continue
        run=$(basename "$d")
        bam="{output.basecalled}/${run}.bam"
        sum="{output.summaries}/${run}_basecall_summary.tsv"
        dorado basecaller "{params.modelname}" "$d" \
          --device cuda:all --recursive --no-trim \
          $([[ -n "{params.modelsdir}" ]] && printf -- "--models-directory %q " "{params.modelsdir}") \
          {params.extra} > "$bam"
        dorado summary "$bam" > "$sum"
      done
    """

rule dorado_demux:
    input: basecalled = rules.dorado_basecall.output.basecalled
    output:
        demuxed   = directory(DEMUX_DIR),
        summaries = directory(os.path.join(SUMMARY_DIR, "demux"))
    threads: R("dorado_demux","threads",8)
    resources:
        mem_mb   = R("dorado_demux","mem_mb",32000),
        runtime  = R("dorado_demux","runtime",120),
        partition= R("dorado_demux","partition","gpu"),
        account  = R("dorado_demux","account","richlab"),
        extra    = R("dorado_demux","extra","--gres=gpu:1")
    params:
        sheet_pat = lambda wc: _expand(config.get("sample_sheet_pattern","")),
        kit       = lambda wc: config.get("barcode_kit","")
    container: CONTAINERS["gpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p "{output.demuxed}" "{output.summaries}"
      shopt -s nullglob
      for bam in "{input.basecalled}"/*.bam; do
        run="$(basename "$bam" .bam)"
        outdir="{output.demuxed}/${run}"
        mkdir -p "$outdir"
        ssp="{params.sheet_pat}"; ssp="${ssp//\{run\}/$run}"
        if [[ -z "$ssp" ]]; then
          sname="{SHEET_NAME}"; sname="${sname//\{run\}/$run}"
          ssp="{SHEET_DIR}/$sname"
        fi
        [[ -r "$ssp" ]] || ssp=""
        dorado demux "$bam" --output-dir "$outdir" \
          $([[ -n "$ssp" ]] && printf -- "--sample-sheet %q " "$ssp") \
          $([[ -n "{params.kit}" ]] && printf -- "--kit-name %q " "{params.kit}") \
          --emit-summary
        if compgen -G "$outdir"/*.txt >/dev/null; then
          mv -f "$outdir"/*.txt "{output.summaries}/${run}_barcoding_summary.txt"
        fi
      done
    """
    
# RAW gate when starting at POD5
if USE_POD5 or (not READS_IN_CFG and Path(POD5_IN).exists()):
    ruleorder: dorado_trim > fastcat_filter
    rule _raw_from_pod5:
        input: _fastqs_from_ckpt
        output: directory(RAW)
        shell: "true"

rule dorado_trim_sample:
    input: bam = bam_for_sample
    output: fastq = os.path.join(RAW, "{sample}.fastq")
    threads: R("dorado_trim","threads",8)
    resources:
        mem_mb   = R("dorado_trim","mem_mb",32000),
        runtime  = R("dorado_trim","runtime",240),
        partition= R("dorado_trim","partition","batch"),
        account  = R("dorado_trim","account","richlab")
    params:
        kit    = lambda wc: config.get("barcode_kit",""),
        emitfq = lambda wc: str(config.get("trim_emit_fastq", True)).lower(),
        minlen = lambda wc: int(config.get("trim_minlen", 0)),
        cont   = CONTAINERS["dorado"],
        skip_samples = tuple(config.get("trim_skip_glob", []))
    log: os.path.join(OUT, "logs", "trim_{sample}.log")
    container: CONTAINERS["gpu"]
    shell: r"""
      set -euo pipefail
      sid="{wildcards.sample}"
      for pat in {params.skip_samples}; do case "$sid" in $pat) echo "skip $sid"; exit 0;; esac; done
      tmp="$(mktemp)"
      apptainer exec --nv --bind /work,/lustre,/mnt/nrdstor,/home "{params.cont}" \
        dorado trim \
          $([[ -n "{params.kit}" ]] && printf -- "--kit-name %q " "{params.kit}") \
          $([[ "{params.emitfq}" == "true" ]] && printf -- "--emit-fastq ") \
          "{input.bam}" > "$tmp"
      if [[ {params.minlen} -gt 0 ]]; then
        awk 'BEGIN{OFS="\n"} NR%4==1{h=$0} NR%4==2{s=$0} NR%4==3{p=$0} NR%4==0{q=$0; if(length(s)>={params.minlen}) print h,s,p,q}' "$tmp" > "{output.fastq}"
        rm -f "$tmp"
      else
        mv "$tmp" "{output.fastq}"
      fi
    """

rule dorado_trim:
    input: fastqs = _fastqs_from_ckpt
    output: touch(os.path.join(TMP, f"trim_done.{DATASET}.ok"))
    shell: "true"

# ---------------- QC & prep (CPU containers) ----------------
rule fastcat_filter:
    input: RAW
    output: fastq = directory(os.path.join(TMP, "filtered"))
    threads: R("fastcat_filter", "threads", 4)
    resources:
        mem_mb = R("fastcat_filter", "mem_mb", 8000),
        runtime = R("fastcat_filter", "runtime", 60),
        partition = R("fastcat_filter", "partition", "guest"),
        account = R("fastcat_filter", "account", "richlab")
    params:
        outdir_base = OUT,
        min_q   = lambda wc: config["min_qscore"],
        minlen  = lambda wc: config["minlength"],
        maxlen  = lambda wc: config["maxlength"],
        filesum = lambda wc: os.path.join(OUT, "qc", "fastcat_file_summary.tsv"),
        readsum = lambda wc: os.path.join(OUT, "qc", "fastcat_read_summary.tsv"),
        histdir = lambda wc: os.path.join(OUT, "qc", "fastcat-histograms"),
        exclude = lambda wc: " ".join(config.get("trim_skip_glob", [])) or "__NONE__"
    log: os.path.join(OUT, "logs/fastcat_filter.log")
    benchmark: os.path.join(OUT, "benchmarks/fastcat_filter.tsv")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {output.fastq} {params.histdir}
      shopt -s nullglob
      for fq in {input}/*.fastq {input}/*.fastq.gz; do
        base=$(basename "$fq")
        for pat in {params.exclude}; do [[ "$pat" == "__NONE__" ]] && break; case "$base" in $pat) continue 2;; esac; done
        stem=$(printf "%s" "$base" | sed -E 's/\.fastq(\.gz)?$//; s/\.fq(\.gz)?$//')
        hdir="{params.histdir}/${stem}"
        filesum="{params.outdir_base}/qc/fastcat_file_summary_${stem}.tsv"
        readsum="{params.outdir_base}/qc/fastcat_read_summary_${stem}.tsv"
        out="{output.fastq}/${stem}.fastq"
        rm -rf "$hdir"
        fastcat --dust --min_qscore {params.min_q} --min_length {params.minlen} --max_length {params.maxlen} \
                --histograms "$hdir" --file "$filesum" --read "$readsum" "$fq" > "$out"
      done
      if ls {params.outdir_base}/qc/fastcat_file_summary_*.tsv >/dev/null 2>&1; then
        awk 'FNR==1 && NR!=1 { next } { print }' {params.outdir_base}/qc/fastcat_file_summary_*.tsv > {params.filesum}
        awk 'FNR==1 && NR!=1 { next } { print }' {params.outdir_base}/qc/fastcat_read_summary_*.tsv > {params.readsum}
      fi
    """

rule nanoplot_qc:
    input: rules.fastcat_filter.output.fastq
    output: directory(os.path.join(OUT, "qc/nanoplot"))
    threads: R("nanoplot_qc", "threads", 2)
    resources:
        mem_mb = R("nanoplot_qc", "mem_mb", 4000),
        runtime = R("nanoplot_qc", "runtime", 30),
        partition = R("nanoplot_qc", "partition", "guest"),
        account = R("nanoplot_qc", "account", "richlab")
    log: os.path.join(OUT, "logs/nanoplot_qc.log")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {output}
      cat {input}/*.fastq > {output}/all.fastq
      NanoPlot --fastq {output}/all.fastq -o {output} --drop_outliers --maxlength {config[maxlength]} --minlength {config[minlength]}
    """

# ---------------- OTU branch ----------------
rule isonclust3:
    input: rules.fastcat_filter.output.fastq
    output: directory(os.path.join(TMP, "OTUs"))
    threads: R("isonclust3", "threads", THREADS)
    resources:
        mem_mb   = R("isonclust3", "mem_mb", 32000),
        runtime  = R("isonclust3", "runtime", 240),
        partition= R("isonclust3", "partition", "batch"),
        account  = R("isonclust3", "account", "richlab")
    log: os.path.join(OUT, "logs/isonclust3.log")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {output}
      shopt -s nullglob
      for fq in {input}/*.fastq; do
        samp=$(basename "$fq" .fastq)
        outdir="{output}/${samp}"
        mkdir -p "$outdir"
        isONclust3 --fastq "$fq" --outfolder "$outdir" --mode ont --post-cluster
      done
    """

rule spoa_consensus:
    input: rules.isonclust3.output
    output: directory(os.path.join(TMP, "consensus_drafts"))
    threads: R("spoa_consensus", "threads", 8)
    resources:
        mem_mb   = R("spoa_consensus", "mem_mb", 16000),
        runtime  = R("spoa_consensus", "runtime", 240),
        partition= R("spoa_consensus", "partition", "batch"),
        account  = R("spoa_consensus", "account", "richlab")
    params:
        max_reads = int(config.get("spoa_max_reads", 500)),
        min_reads = int(config.get("spoa_min_reads", 3)),
        extra     = lambda wc: config.get("spoa_extra","")
    log: os.path.join(OUT, "logs/spoa_consensus.log")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {output}
      # For each cluster FASTQ, cap reads to avoid pathological runtimes.
      find {input} -type f -path "*/clustering/fastq_files/*.fastq" | while read -r fq; do
        sample=$(basename "$(dirname "$(dirname "$fq")")")
        cid=$(basename "$fq" .fastq)
        out="{output}/${sample}_${cid}.fasta"
        # count reads; skip tiny clusters
        n=$(awk 'END{print NR/4}' "$fq")
        if (( n < {params.min_reads} )); then continue; fi
        tmpf=$(mktemp)
        if (( n > {params.max_reads} )); then
          # deterministic subsample: take first max_reads (fast, no extra deps)
          awk -v m={params.max_reads} 'NR%4==1{c++} c<=m{print}' "$fq" > "$tmpf"
        else
          cp "$fq" "$tmpf"
        fi
        # default scoring is fine for 16S; add {params.extra} to tweak if needed
        spoa {params.extra} "$tmpf" > "$out"
        rm -f "$tmpf"
      done
    """

rule vsearch_pool_cluster:
    input: rules.spoa_consensus.output
    output:
        drafts = os.path.join(TMP, "pooled/all_draft_otus.fasta"),
        cent99 = os.path.join(OUT, "otu/otus_centroids_99.fasta"),
        cent97 = os.path.join(OUT, "otu/otus_centroids_97.fasta")
    threads: R("vsearch_pool_cluster", "threads", THREADS)
    resources:
        mem_mb   = R("vsearch_pool_cluster", "mem_mb", 32000),
        runtime  = R("vsearch_pool_cluster", "runtime", 300),
        partition= R("vsearch_pool_cluster", "partition", "batch"),
        account  = R("vsearch_pool_cluster", "account", "richlab")
    log: os.path.join(OUT, "logs/vsearch_pool_cluster.log")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p "$(dirname {output.drafts})" "$(dirname {output.cent99})" "{TMP}/pooled"
      cat {input}/*.fasta > {output.drafts}
      vsearch --derep_fulllength {output.drafts} --sizeout --relabel OTU_ --strand both \
              --minuniquesize {MIN_UNIQUE} --threads {threads} \
              --output {TMP}/pooled/otus_derep.fasta
      vsearch --cluster_fast {TMP}/pooled/otus_derep.fasta --id {config[otu_id_primary]} \
              --centroids {output.cent99} --threads {threads}
      vsearch --cluster_fast {TMP}/pooled/otus_derep.fasta --id {config[otu_id_legacy]} \
              --centroids {output.cent97} --threads {threads}
    """

# Gather reads and polishing
rule map_all_reads:
    input:
        reads_dir = os.path.join(TMP, "filtered"),
        ref_99    = os.path.join(OUT, "otu/otus_centroids_99.fasta")
    output:
        fq   = ALL_READS_FQ,
        bam  = MAP_BAM_99
    threads: R("map_all_reads", "threads", THREADS)
    resources:
        mem_mb   = R("map_all_reads", "mem_mb", 16000),
        runtime  = R("map_all_reads", "runtime", 180),
        partition= R("map_all_reads", "partition", "batch"),
        account  = R("map_all_reads", "account", "richlab")
    params:
        tmp = TMP, outdir = OUT
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {POLISH_DIR}
      cat {input.reads_dir}/*.fastq > {output.fq}
      minimap2 -t {threads} -ax map-ont {input.ref_99} {output.fq} | samtools sort -@ {threads} -o {output.bam}
      samtools index {output.bam}
    """

rule racon_round1:
    input: reads = ALL_READS_FQ, bam = MAP_BAM_99, ref = os.path.join(OUT, "otu/otus_centroids_99.fasta")
    output: r1 = R1_FASTA
    threads: R("racon_round1", "threads", THREADS)
    resources:
        mem_mb   = R("racon_round1", "mem_mb", 16000),
        runtime  = R("racon_round1", "runtime", 120),
        partition= R("racon_round1", "partition", "batch"),
        account  = R("racon_round1", "account", "richlab")
    container: CONTAINERS["cpu"]
    shell: r""" set -euo pipefail; racon -t {threads} {input.reads} {input.bam} {input.ref} > {output.r1} """

rule map_r1:
    input: reads = ALL_READS_FQ, r1 = R1_FASTA
    output: bam = MAP_BAM_R1
    threads: R("map_r1", "threads", THREADS)
    resources:
        mem_mb   = R("map_r1", "mem_mb", 16000),
        runtime  = R("map_r1", "runtime", 180),
        partition= R("map_r1", "partition", "batch"),
        account  = R("map_r1", "account", "richlab")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      minimap2 -t {threads} -ax map-ont {input.r1} {input.reads} | samtools sort -@ {threads} -o {output.bam}
      samtools index {output.bam}
    """

rule racon_round2:
    input: reads = ALL_READS_FQ, bam = MAP_BAM_R1, r1 = R1_FASTA
    output: r2 = R2_FASTA
    threads: R("racon_round2", "threads", THREADS)
    resources:
        mem_mb   = R("racon_round2", "mem_mb", 16000),
        runtime  = R("racon_round2", "runtime", 120),
        partition= R("racon_round2", "partition", "batch"),
        account  = R("racon_round2", "account", "richlab")
    container: CONTAINERS["cpu"]
    shell: r""" set -euo pipefail; racon -t {threads} {input.reads} {input.bam} {input.r1} > {output.r2} """

# Medaka (GPU via explicit --nv)
rule medaka_polish:
    input: reads = ALL_READS_FQ, draft = R2_FASTA
    output: polished = POLISHED
    threads: R("medaka_polish", "threads", THREADS)
    resources:
        mem_mb   = R("medaka_polish", "mem_mb", 16000),
        runtime  = R("medaka_polish", "runtime", 240),
        partition= R("medaka_polish", "partition", "gpu"),
        account  = R("medaka_polish", "account", "richlab"),
        extra    = R("medaka_polish", "extra", "--gres=gpu:1")
    params:
        medaka_model = lambda wc: config["medaka_model"]
    container: CONTAINERS["gpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {POLISH_DIR}/medaka_refined
      medaka_consensus -i {input.reads} -d {input.draft} \
                       -o {POLISH_DIR}/medaka_refined \
                       -m {params.medaka_model} --bacteria
      cp {POLISH_DIR}/medaka_refined/consensus.fasta {output.polished}
    """

# Taxonomy/tree
rule chimera_taxonomy_tree:
    input: fasta = POLISHED, db = ITGDB_UDB
    output:
        nonchim = os.path.join(OUT, "otu/otus_clean.fasta"),
        chimera = os.path.join(OUT, "otu/otus_chimeras.fasta"),
        sintax  = os.path.join(OUT, "otu/otus_taxonomy.sintax"),
        msa     = os.path.join(OUT, "otu/otu_references_aligned.fasta")
    threads: R("chimera_taxonomy_tree", "threads", THREADS)
    resources:
        mem_mb   = R("chimera_taxonomy_tree", "mem_mb", 32000),
        runtime  = R("chimera_taxonomy_tree", "runtime", 240),
        partition= R("chimera_taxonomy_tree", "partition", "batch"),
        account  = R("chimera_taxonomy_tree", "account", "richlab")
    log: os.path.join(OUT, "logs/chimera_taxonomy_tree.log")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {OUT}/otu
      vsearch --uchime_denovo {input.fasta} --nonchimeras {output.nonchim} --chimeras {output.chimera} --threads {threads}
      vsearch --sintax {output.nonchim} --db {input.db} --sintax_cutoff {SINTAX_CUTOFF} --tabbedout {output.sintax} --threads {threads}
      mafft --auto --thread {threads} {output.nonchim} > {output.msa}
    """

rule iqtree2_tree:
    input: msa = rules.chimera_taxonomy_tree.output.msa
    output: tree = os.path.join(OUT, "otu/otu_tree.treefile")
    threads: R("iqtree2_tree", "threads", THREADS)
    resources:
        mem_mb   = R("iqtree2_tree", "mem_mb", 64000),
        runtime  = R("iqtree2_tree", "runtime", 720),
        partition= R("iqtree2_tree", "partition", "batch"),
        account  = R("iqtree2_tree", "account", "richlab")
    log: os.path.join(OUT, "logs/iqtree2_tree.log")
    container: CONTAINERS["iqtree2"]
    shell: r"""
      set -euo pipefail
      iqtree2 -s {input.msa} -nt AUTO -m TEST -bb 1000 -alrt 1000 -pre {OUT}/otu/otu_tree
      test -s {output.tree}
    """

# OTU table & merge
rule otu_table_per_sample:
    input: refs = rules.chimera_taxonomy_tree.output.nonchim, reads = rules.fastcat_filter.output.fastq
    output: merged = os.path.join(OUT, "otu/otu_table_merged.tsv")
    threads: R("otu_table_per_sample", "threads", THREADS)
    resources:
        mem_mb   = R("otu_table_per_sample", "mem_mb", 16000),
        runtime  = R("otu_table_per_sample", "runtime", 180),
        partition= R("otu_table_per_sample", "partition", "batch"),
        account  = R("otu_table_per_sample", "account", "richlab")
    log: os.path.join(OUT, "logs/otu_table_per_sample.log")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {OUT}/otu/tables
      shopt -s nullglob
      for fq in {input.reads}/*.fastq; do
        sid=$(basename "$fq" .fastq)
        vsearch --usearch_global "$fq" --db {input.refs} --id {MAP_ID} --strand {STRAND} \
                --otutabout {OUT}/otu/tables/otu_table_${sid}.tsv --threads {threads}
      done
      python - <<'PY'
      import glob, os, pandas as pd
      out = r"{output.merged}"
      tables = glob.glob(os.path.join(r"{OUT}", "otu", "tables", "otu_table_*.tsv"))
      dfs = [pd.read_csv(t, sep="\t") for t in tables]
      if not dfs:
          raise SystemExit("No per-sample OTU tables found to merge.")
      for d in dfs:
          first = d.columns[0]
          d.rename(columns={first: "OTU"}, inplace=True)
      from functools import reduce
      merged = reduce(lambda l, r: pd.merge(l, r, on="OTU", how="outer"), dfs).fillna(0)
      merged.to_csv(out, sep="\t", index=False)
      PY
    """

# ASV branch (NanoASV)
rule asv_nanoasv:
    input: rules.fastcat_filter.output.fastq
    output: os.path.join(OUT, "asv/nanoasv/phyloseq.RData")
    threads: R("asv_nanoasv", "threads", THREADS)
    resources:
        mem_mb   = R("asv_nanoasv", "mem_mb", 32000),
        runtime  = R("asv_nanoasv", "runtime", 360),
        partition= R("asv_nanoasv", "partition", "batch"),
        account  = R("asv_nanoasv", "account", "richlab")
    log: os.path.join(OUT, "logs/asv_nanoasv.log")
    container: CONTAINERS["nanoasv"]
    run:
        if config.get("asv_method", None) != "nanoasv":
            shell("mkdir -p {OUT}/asv/nanoasv && : > {OUT}/asv/nanoasv/phyloseq.RData")
        else:
            shell(r"""
              set -euo pipefail
              mkdir -p {OUT}/asv/nanoasv
              nanoasv --dir {input} --out {OUT}/asv/nanoasv \
                      --reference {SILVA_FASTA} \
                      --subsampling {config[nanoasv_opts][subsample_per_barcode]} \
                      --samtools-qual {config[nanoasv_opts][mapq]}
            """)
