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

# --- helper to escape braces so Snakemake doesn't try to format run ---
def esc_braces(s):
    return s.replace("{", "{{").replace("}", "}}") if isinstance(s, str) else s

def _require_abs_resolved(p, name):
    if not p or "$" in p:
        raise WorkflowError(f"{name} is unset or contains an unresolved '$': got {p!r}. Did you export {name}?")
    p = os.path.expanduser(p)
    if not os.path.isabs(p):
        p = os.path.join(PROJ, p)
    return p
  
def fmt_template(s, **vals):
    if not isinstance(s, str): return s
    s = os.path.expandvars(s)
    tmpl = string.Template(s.replace("{", "${").replace("}", "}"))
    return tmpl.safe_substitute(**vals)

def resolve_path(base, path_template, **vals):
    p = fmt_template(path_template, **vals)
    if not p: return ""
    return p if os.path.isabs(p) else os.path.join(base, p)
  
def Rq(rule, key):
    """Required resource getter: error if missing in config."""
    try:
        return config["resources"][rule][key]
    except KeyError:
        raise WorkflowError(f"Missing config.resources['{rule}']['{key}']")

def R(rule, key, default=""):
    """Optional resource getter with empty-string default (e.g. for 'extra')."""
    return config.get("resources", {}).get(rule, {}).get(key, default)

  
# --- dataset & layout ---
SAMPLESET = _expand(config.get("sampleset", "")).strip()
DATASET   = _expand(config.get("dataset", "")).strip()
if not DATASET:
    raise WorkflowError("config.dataset is required")

# --- require out_root to be resolved and absolute ---
OUT_ROOT  = _expand(config.get("out_root", None))
if not OUT_ROOT or "$" in OUT_ROOT:
    raise WorkflowError(f"Config 'out_root' missing or unresolved: {OUT_ROOT!r}")
OUT_ROOT = os.path.expanduser(OUT_ROOT)
if not os.path.isabs(OUT_ROOT):
    raise WorkflowError(f"Config 'out_root' must be absolute: {OUT_ROOT!r}")

LAYOUT = { **{
    "dataset_dir":     "{sampleset}/{dataset}",
    "raw_dir":         "{sampleset}/{dataset}/raw",          # dataset workspace
    "pod5_dir":        "{sampleset}/pod5",                   # run parents
    "basecall_dir":    "{sampleset}/basecalled/{run}",       # per-run
    "demux_dir":       "{sampleset}/demuxed/{run}",          # per-run
    "summary_dir":     "{sampleset}/dorado_summaries/{run}", # per-run
    "sample_sheet_dir": "{sampleset}/samples",
    "sample_sheet_name": "{run}_sample_sheet.csv",
    "sample_routing":  "{sampleset}/samples/sample_to_dataset.tsv",
}, **config.get("layout", {}) }

# Dataset workspace (everything downstream lands here)
DSET = resolve_path(OUT_ROOT, LAYOUT["dataset_dir"], sampleset=SAMPLESET, dataset=DATASET)
OUT  = DSET
TMP  = os.path.join(DSET, "tmp")

# Inputs / upstream per-run outputs
def pod5_dir(run): return resolve_path(OUT_ROOT, os.path.join(LAYOUT["pod5_dir"], run), sampleset=SAMPLESET, dataset=DATASET, run=run)
def basecall_dir(run): return resolve_path(OUT_ROOT, LAYOUT["basecall_dir"], sampleset=SAMPLESET, dataset=DATASET, run=run)
def demux_dir(run):    return resolve_path(OUT_ROOT, LAYOUT["demux_dir"],    sampleset=SAMPLESET, dataset=DATASET, run=run)
def summary_dir(run):  return resolve_path(OUT_ROOT, LAYOUT["summary_dir"],  sampleset=SAMPLESET, dataset=DATASET, run=run)

# Where demux_trim puts the dataset subset
RAW_BASE = resolve_path(OUT_ROOT, LAYOUT["raw_dir"], sampleset=SAMPLESET, dataset=DATASET)
def raw_dir_for_run(run): return os.path.join(RAW_BASE, run)

# Samples / sheets
SHEET_DIR  = resolve_path(OUT_ROOT, LAYOUT["sample_sheet_dir"],  sampleset=SAMPLESET, dataset=DATASET)
SHEET_NAME = LAYOUT["sample_sheet_name"]
ROUTING_TSV = resolve_path(OUT_ROOT, LAYOUT["sample_routing"], sampleset=SAMPLESET, dataset=DATASET)

# Ensure base dirs (dataset-scoped)
for p in (OUT, TMP, os.path.join(OUT,"otu"), os.path.join(OUT,"asv"), os.path.join(OUT,"logs"), os.path.join(OUT,"benchmarks")):
    Path(p).mkdir(parents=True, exist_ok=True)

# Containers
CONTAINERS = {
    "cpu":     _expand(config.get("container_cpu",     "/mnt/nrdstor/richlab/shared/containers/nanomb.sif")),
    "gpu":     _expand(config.get("container_gpu",     "/mnt/nrdstor/richlab/shared/containers/nanombgpu.sif")),
    "nanoasv": _expand(config.get("container_nanoasv", "/mnt/nrdstor/richlab/shared/containers/nanoasv.sif")),
    "nanoalign": _expand(config.get("container_nanoalign", "/mnt/nrdstor/richlab/shared/containers/nanoalign.sif")),
    "dorado":  _expand(config.get("container_dorado",  "/mnt/nrdstor/richlab/shared/containers/dorado.sif")),
    "mafft":  _expand(config.get("container_mafft",  "/mnt/nrdstor/richlab/shared/containers/mafft.sif")),
    "iqtree3":  _expand(config.get("container_iqtree3",  "/mnt/nrdstor/richlab/shared/containers/iqtree3.sif"))
}

# ---- wildcardable string templates (no callables) ----
BASECALL_DIR_T   = resolve_path(OUT_ROOT, LAYOUT["basecall_dir"],
                                sampleset=SAMPLESET, dataset=DATASET, run="{run}")
DEMUX_DIR_T      = resolve_path(OUT_ROOT, LAYOUT["demux_dir"],
                                sampleset=SAMPLESET, dataset=DATASET, run="{run}")
SUMMARY_DIR_T    = resolve_path(OUT_ROOT, LAYOUT["summary_dir"],
                                sampleset=SAMPLESET, dataset=DATASET, run="{run}")
RAW_DIR_T        = os.path.join(RAW_BASE, "{run}")
DEMUX_INDEX_T    = os.path.join(TMP, "demux_index_{run}.tsv")

LOG_BASECALL_T   = os.path.join(OUT, "logs", "dorado_basecall_{run}.log")
LOG_TRIM_T       = os.path.join(OUT, "logs", "trim_{run}_{sample}.log")

READS_IN_CFG = _expand(config.get("reads_in","")).strip()
POD5_IN_CFG  = _expand(config.get("pod5_in","")).strip()

ITGDB_UDB   = _expand(config.get("itgdb_udb", ""))
SILVA_FASTA = _expand(config.get("silva_fasta", ""))

MAP_ID  = float(config.get("map_id", 0.98))
STRAND  = config.get("strand", "both")
NANOASV = config.get("nanoasv_bin", "nanoasv")
SINTAX_CUTOFF = float(config.get("sintax_cutoff", 0.8))
MIN_UNIQUE = int(config.get("min_unique_size", 1))

# Polishing paths
POLISH_DIR   = os.path.join(TMP, "polished")
POLISHED     = os.path.join(POLISH_DIR, "polished_otus.fasta")
OTU_CENTROIDS_FASTA  = os.path.join(OUT, "otu/otus_centroids_99.fasta")
ALL_READS_FQ = os.path.join(POLISH_DIR, "all_reads.fastq")
MAP_BAM_R0   = os.path.join(POLISH_DIR, "map_r0.bam")
MAP_BAM_R1   = os.path.join(POLISH_DIR, "map_r1.bam")
R1_FASTA     = os.path.join(POLISH_DIR, "r1.fasta")
R2_FASTA     = os.path.join(POLISH_DIR, "r2.fasta")

# ---- runs: discover or use config ----
def discover_runs():
    base = resolve_path(OUT_ROOT, LAYOUT["pod5_dir"], sampleset=SAMPLESET, dataset=DATASET)
    gpat = (config.get("run_glob") or "*").strip()
    rre  = (config.get("run_regex") or "").strip()

    cands = []
    if Path(base).exists():
        cands = [p.name for p in Path(base).iterdir() if p.is_dir()]
    if gpat and gpat != "*":
        import fnmatch
        cands = [r for r in cands if fnmatch.fnmatch(r, gpat)]
    if rre:
        import re
        rx = re.compile(rre)
        cands = [r for r in cands if rx.search(r)]
    return sorted(cands)

RUNS = list(map(str, config.get("runs", []) or discover_runs()))
if not RUNS:
    raise WorkflowError("No runs found. Set config.runs or ensure pod5 folders exist under layout.pod5_dir.")


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

# ---- Build final targets safely (no functions leak into input) ----
_demux_done = [os.path.join(raw_dir_for_run(r), "demux_trim.done") for r in RUNS]

_asv = asv_target()                 
if not _asv:
    _asv = []
elif isinstance(_asv, str):
    _asv = [_asv]                   

_final_targets = (
    _demux_done
    + [
        os.path.join(TMP, "preflight.ok"),
        os.path.join(TMP, "dorado_all_runs.ok"),
        os.path.join(OUT, "manifest.txt"),
        os.path.join(OUT, "benchmarks/fastcat_filter.tsv"),
        os.path.join(OUT, "qc/nanoplot"),
        POLISHED,
        os.path.join(OUT, "otu/otu_table_merged.tsv"),
        os.path.join(OUT, "otu/otu_references_aligned.fasta"),
        os.path.join(OUT, "otu/otu_tree.treefile"),
        os.path.join(OUT, "otu/otus_taxonomy.sintax"),
        os.path.join(OUT, "otu/otus_centroids_99.fasta"),
        os.path.join(OUT, "otu/otus_centroids_97.fasta"),
        os.path.join(OUT, "silva/species/silva_species_merged.tsv"),  
      ]
    + _asv
)

rule all:
    input: _final_targets

rule preflight:
    input: db = ITGDB_UDB
    output: touch(os.path.join(TMP, "preflight.ok"))
    container: CONTAINERS["cpu"]
    run:
        if not input.db or not os.path.exists(input.db) or os.path.getsize(input.db) == 0:
            raise WorkflowError(f"SINTAX DB missing/empty: {input.db!r}")

        if not SILVA_FASTA or not os.path.exists(SILVA_FASTA) or os.path.getsize(SILVA_FASTA) == 0:
            raise WorkflowError(f"SILVA reference missing/empty: {SILVA_FASTA!r}")

        must_have = ("cpu","gpu","nanoalign","dorado","mafft","iqtree3")
        for k in must_have:
            img = CONTAINERS.get(k, "")
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
    input:
        pod5 = lambda wc: pod5_dir(wc.run)    
    output:
        basecalled = directory(BASECALL_DIR_T),
        summaries  = directory(os.path.join(SUMMARY_DIR_T, "basecall"))
    threads: Rq("dorado_basecall", "threads")
    resources:
        mem_mb   = Rq("dorado_basecall", "mem_mb"), 
        runtime  = Rq("dorado_basecall", "runtime"),
        partition= Rq("dorado_basecall", "partition"),
        account  = Rq("dorado_basecall", "account"),
        extra    =  R("dorado_basecall", "extra") 
    params:
        modelname = lambda wc: config.get("dorado_model_name","sup"),
        modelsdir = lambda wc: _expand(config.get("dorado_models_dir","/models")),
        extra     = lambda wc: config.get("dorado_extra","")
    log: LOG_BASECALL_T                        
    container: CONTAINERS["dorado"]
    shell: r"""
      set -euo pipefail
      mkdir -p "{output.basecalled}" "{output.summaries}"
      bam="{output.basecalled}/{wildcards.run}.bam"
      sum="{output.summaries}/{wildcards.run}_basecall_summary.tsv"
      dorado basecaller "{params.modelname}" "{input.pod5}" \
        --device cuda:all --recursive --no-trim \
        $([[ -n "{params.modelsdir}" ]] && printf -- "--models-directory %q " "{params.modelsdir}") \
        {params.extra} > "$bam"
      dorado summary "$bam" > "$sum"
    """

rule dorado_demux:
    input:
        basecalled = BASECALL_DIR_T            
    output:
        demuxed   = directory(DEMUX_DIR_T),
        summaries = directory(os.path.join(SUMMARY_DIR_T, "demux")),
    threads: Rq("dorado_demux", "threads")
    resources:
        mem_mb   = Rq("dorado_demux", "mem_mb"), 
        runtime  = Rq("dorado_demux", "runtime"),
        partition= Rq("dorado_demux", "partition"),
        account  = Rq("dorado_demux", "account"),
        extra    =  R("dorado_demux", "extra") 
    params:
        sheet_pat  = lambda wc: esc_braces((_expand(config.get("sample_sheet_pattern","")) or "").replace("{run}", "___RUN___")),
        sheet_name = lambda wc: esc_braces(SHEET_NAME.replace("{run}", "___RUN___")),
        sheet_dir  = lambda wc: esc_braces(SHEET_DIR),
        kit        = lambda wc: config.get("barcode_kit",""),
    container: CONTAINERS["dorado"]
    shell: r"""
      set -euo pipefail
      mkdir -p "{output.demuxed}" "{output.summaries}"
      bam="{input.basecalled}/{wildcards.run}.bam"
      outdir="{output.demuxed}"
      run_id="{wildcards.run}"
      ssp_pat="{params.sheet_pat}"
      if [[ -n "$ssp_pat" ]]; then
        ssp="$(printf '%s' "$ssp_pat" | sed "s/___RUN___/$run_id/g")"
      else
        sname_tmpl="{params.sheet_name}"
        sname="$(printf '%s' "$sname_tmpl" | sed "s/___RUN___/$run_id/g")"
        ssp="{params.sheet_dir}/$sname"
      fi
      [[ -r "$ssp" ]] || ssp=""
      dorado demux "$bam" --output-dir "$outdir" \
        $([[ -n "$ssp" ]] && printf -- "--sample-sheet %q " "$ssp") \
        $([[ -n "{params.kit}" ]] && printf -- "--kit-name %q " "{params.kit}") \
        --emit-summary
      if compgen -G "$outdir"/*.txt >/dev/null; then
        mv -f "$outdir"/*.txt "{output.summaries}/{wildcards.run}_barcoding_summary.txt"
      fi
    """   
    

rule dorado_all_runs:
    input:
        [basecall_dir(r) for r in RUNS],
        [demux_dir(r)    for r in RUNS]
    output:
        touch(os.path.join(TMP, "dorado_all_runs.ok"))
    shell: "true"
        
def demux_index_path_for_run(run): return os.path.join(TMP, f"demux_index_{run}.tsv")

rule demux_index_one_run:
    input: demux = DEMUX_DIR_T
    output: tsv = DEMUX_INDEX_T
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      out="{output.tsv}"
      mkdir -p "$(dirname "$out")"
      printf "sample\trun\tbam\n" > "$out"
      shopt -s nullglob
      for bam in "{input.demux}"/*.bam; do
        base="$(basename "$bam" .bam)"
        prefix="{wildcards.run}_"
        sample="$(printf '%s' "$base" | sed -E "s/^$prefix//")"
        printf "%s\t%s\t%s\n" "$sample" "{wildcards.run}" "$bam" >> "$out"
      done
    """
    
rule demux_index_all:
    input: [demux_index_path_for_run(r) for r in RUNS]
    output: tsv = os.path.join(TMP, "demux_index.tsv")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      cat {input} | awk 'NR==1 || FNR>1' > {output.tsv}
    """
    
def bam_for_sample_run(wc):
    idx = rules.demux_index_all.output.tsv
    wanted = []
    import csv as _csv
    with open(idx) as fh:
        rdr = _csv.DictReader(fh, delimiter="\t")
        for row in rdr:
            if row["run"] == wc.run and row["sample"] == wc.sample:
                wanted.append(row["bam"])
    if not wanted:
        raise WorkflowError(f"No BAM for run={wc.run} sample={wc.sample} in {idx}")
    return sorted(wanted)[-1]

rule dorado_trim_sample:
    input: bam = bam_for_sample_run          
    output: fastq = os.path.join(RAW_DIR_T, "{sample}.fastq")   
    threads: Rq("dorado_trim", "threads")
    resources:
        mem_mb   = Rq("dorado_trim", "mem_mb"), 
        runtime  = Rq("dorado_trim", "runtime"),
        partition= Rq("dorado_trim", "partition"),
        account  = Rq("dorado_trim", "account"),
        extra    =  R("dorado_trim", "extra") 
    params:
        kit    = lambda wc: config.get("barcode_kit",""),
        emitfq = lambda wc: str(config.get("trim_emit_fastq", True)).lower(),
        minlen = lambda wc: int(config.get("trim_minlen", 0)),
        skip_samples = lambda wc: " ".join(config.get("trim_skip_glob", [])) or "__NONE__"
    log: LOG_TRIM_T
    container: CONTAINERS["dorado"]
    shell: r"""
      set -euo pipefail
      mkdir -p "$(dirname "{output.fastq}")"
      sid="{wildcards.sample}"
      for pat in {params.skip_samples}; do
        [[ "$pat" == "__NONE__" ]] && break
        case "$sid" in $pat) echo "skip $sid"; exit 0;;
        esac
      done
      tmp="$(mktemp)"
      dorado trim \
        $([[ -n "{params.kit}" ]] && printf -- "--kit-name %q " "{params.kit}") \
        $([[ "{params.emitfq}" == "true" ]] && printf -- "--emit-fastq ") \
        "{input.bam}" > "$tmp"
      if [[ {params.minlen} -gt 0 ]]; then
        awk 'BEGIN{{OFS="\n"}} NR%4==1{{h=$0}} NR%4==2{{s=$0}} NR%4==3{{p=$0}} NR%4==0{{q=$0; if(length(s)>={params.minlen}) print h,s,p,q}}' "$tmp" > "{output.fastq}"
        rm -f "$tmp"
      else
        mv "$tmp" "{output.fastq}"
      fi
    """
    
rule dorado_trim_run_done:
    input:
        fastqs = lambda wc: expand(os.path.join(raw_dir_for_run(wc.run), "{sample}.fastq"),
                                   sample=[p.stem.replace(f"{wc.run}_","") for p in Path(demux_dir(wc.run)).glob("*.bam")])
    output: touch(os.path.join(RAW_DIR_T, "demux_trim.done"))
    shell: "true"

# ---------------- QC & prep (CPU containers) ----------------
def all_raw_fastq_glob():
    files = []
    if Path(RAW_BASE).exists():
        for r in RUNS:
            d = raw_dir_for_run(r)
            files.extend(glob.glob(os.path.join(d, "*.fastq")))
            files.extend(glob.glob(os.path.join(d, "*.fastq.gz")))
    return sorted(set(files))
  
    
rule fastcat_filter:
    input: all_raw_fastq_glob()
    output: fastq = directory(os.path.join(TMP, "filtered"))
    threads: Rq("fastcat_filter", "threads")
    resources:
        mem_mb   = Rq("fastcat_filter", "mem_mb"), 
        runtime  = Rq("fastcat_filter", "runtime"),
        partition= Rq("fastcat_filter", "partition"),
        account  = Rq("fastcat_filter", "account"),
        extra    =  R("fastcat_filter", "extra") 
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
      for fq in {input}; do
        base=$(basename "$fq")
        for pat in {params.exclude}; do [[ "$pat" == "__NONE__" ]] && break; case "$base" in $pat) continue 2;; esac; done
        stem=$(printf "%s" "$base" | sed -E 's/\.fastq(\.gz)?$//; s/\.fq(\.gz)?$//')
        hdir="{params.histdir}/${{stem}}"
        filesum="{params.outdir_base}/qc/fastcat_file_summary_${{stem}}.tsv"
        readsum="{params.outdir_base}/qc/fastcat_read_summary_${{stem}}.tsv"
        out="{output.fastq}/${{stem}}.fastq"
        rm -rf "$hdir"
        fastcat --dust --min_qscore {params.min_q} --min_length {params.minlen} --max_length {params.maxlen} \
                --histograms "$hdir" --file "$filesum" --read "$readsum" "$fq" > "$out"
      done
      if ls {params.outdir_base}/qc/fastcat_file_summary_*.tsv >/dev/null 2>&1; then
        awk 'FNR==1 && NR!=1 {{ next }} {{ print }}' {params.outdir_base}/qc/fastcat_file_summary_*.tsv > {params.filesum}
        awk 'FNR==1 && NR!=1 {{ next }} {{ print }}' {params.outdir_base}/qc/fastcat_read_summary_*.tsv > {params.readsum}
      fi
    """
    
rule nanoplot_qc:
    input: rules.fastcat_filter.output.fastq
    output: directory(os.path.join(OUT, "qc/nanoplot"))
    threads: Rq("nanoplot_qc", "threads")
    resources:
        mem_mb   = Rq("nanoplot_qc", "mem_mb"), 
        runtime  = Rq("nanoplot_qc", "runtime"),
        partition= Rq("nanoplot_qc", "partition"),
        account  = Rq("nanoplot_qc", "account"),
        extra    =  R("nanoplot_qc", "extra") 
    params:
      maxlen = lambda wc: config["maxlength"],
      minlen = lambda wc: config["minlength"]
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {output}
      cat {input}/*.fastq > {output}/all.fastq
      NanoPlot --fastq {output}/all.fastq -o {output} --drop_outliers \
        --maxlength {params.maxlen} --minlength {params.minlen}
    """

# ---------------- OTU branch ----------------
rule silva_index:
    input:
        fasta = SILVA_FASTA
    output:
        mmi = os.path.join(OUT, "silva/index", config.get("silva_index_name", "SILVA_138.2_SSURef_NR99.mmi"))
    threads: Rq("silva_index", "threads")
    resources:
        mem_mb   = Rq("silva_index", "mem_mb"), 
        runtime  = Rq("silva_index", "runtime"),
        partition= Rq("silva_index", "partition"),
        account  = Rq("silva_index", "account"),
        extra    =  R("silva_index", "extra") 
    container: CONTAINERS["nanoalign"]  
    shell: r"""
      set -euo pipefail
      mkdir -p "{OUT}/silva/index"
      minimap2 -d "{output.mmi}" "{input.fasta}"
    """
    
rule silva_taxmap:
    input:
        fasta = SILVA_FASTA
    output:
        taxmap = os.path.join(OUT, "silva", "silva_taxmap.tsv")
    threads: Rq("silva_taxmap", "threads")
    resources:
        mem_mb   = Rq("silva_taxmap", "mem_mb"), 
        runtime  = Rq("silva_taxmap", "runtime"),
        partition= Rq("silva_taxmap", "partition"),
        account  = Rq("silva_taxmap", "account"),
        extra    =  R("silva_taxmap", "extra") 
    container: CONTAINERS["cpu"]         # needs Python
    shell: r"""
      set -euo pipefail
      mkdir -p "{OUT}/silva"
      PYBIN=$(command -v python || command -v python3 || true)
      if [ -z "$PYBIN" ]; then echo "No python in container." >&2; exit 127; fi

      "$PYBIN" - <<'PY'
      import sys
      out = r"{output.taxmap}"
      fasta = r"{input.fasta}"
      deflines = []
      with open(fasta, "r", encoding="utf-8", errors="ignore") as fh:
          for line in fh:
              if line.startswith(">"):
                  deflines.append(line[1:].strip())

      with open(out, "w") as w:
          w.write("id\ttaxonomy\n")
          for d in deflines:
              parts = d.split()
              sid = parts[0]
              tax = d[len(sid):].strip()
              if not tax:
                  tax = "NA"
              w.write(f"{sid}\t{tax}\n")
      PY
    """
    
rule silva_map_reads:
    input:
        idx   = rules.silva_index.output.mmi,
        reads = rules.fastcat_filter.output.fastq   
    output:
        done = os.path.join(OUT, "silva/paf", ".done")
    threads:   Rq("silva_map_reads", "threads")
    resources:
        mem_mb    = Rq("silva_map_reads", "mem_mb"),
        runtime   = Rq("silva_map_reads", "runtime"),
        partition = Rq("silva_map_reads", "partition"),
        account   = Rq("silva_map_reads", "account")
    container: CONTAINERS["nanoalign"]
    params:
        preset  = config["silva_minimap"]["preset"],
        prim    = "--secondary=no -N 1" if config["silva_minimap"].get("primary_only", True) else "",
    shell: r"""
      set -euo pipefail
      mkdir -p "{OUT}/silva/paf"
      shopt -s nullglob

      for fq in {input.reads}/*.fastq {input.reads}/*.fastq.gz; do
        [ -e "$fq" ] || continue
        bn=$(basename "$fq")
        sid="${{bn%.fastq}}"; sid="${{sid%.fastq.gz}}"
        echo "minimap2 → $sid" >&2
        minimap2 -t {threads} -x "{params.preset}" {params.prim} \
                 "{input.idx}" "$fq" > "{OUT}/silva/paf/${{sid}}.paf"
      done

      touch "{output.done}"
    """
    
rule silva_species_tables:
    input:
        paf_done = rules.silva_map_reads.output.done,
        taxmap   = rules.silva_taxmap.output.taxmap
    output:
        merged   = os.path.join(OUT, "silva/species", "silva_species_merged.tsv")
    threads: Rq("silva_species_tables", "threads")
    resources:
        mem_mb   = Rq("silva_species_tables", "mem_mb"), 
        runtime  = Rq("silva_species_tables", "runtime"),
        partition= Rq("silva_species_tables", "partition"),
        account  = Rq("silva_species_tables", "account"),
        extra    =  R("silva_species_tables", "extra") 
    container: CONTAINERS["cpu"]
    params:
        mapq_min   = config["silva_minimap"]["mapq_min"],
        aln_min_bp = config["silva_minimap"]["aln_min_bp"]
    shell: r"""
      set -euo pipefail
      mkdir -p "{OUT}/silva/species"
      PYBIN=$(command -v python || command -v python3 || true)
      if [ -z "$PYBIN" ]; then echo "No python in container." >&2; exit 127; fi

      "$PYBIN" - <<'PY'
      import os, glob, csv, sys, re
      from collections import Counter, defaultdict

      paf_dir   = r"{OUT}/silva/paf"
      taxmap_fn = r"{input.taxmap}"
      out_merged = r"{output.merged}"
      mapq_min   = int("{params.mapq_min}")
      aln_min_bp = int("{params.aln_min_bp}")

      id2tax = {{}}
      with open(taxmap_fn) as fh:
          next(fh)
          for line in fh:
              sid, tax = line.rstrip("\n").split("\t", 1)
              id2tax[sid] = tax

      def species_from_tax(tax: str) -> str:
          if not tax or tax == "NA":
              return "Unassigned"
          if ";" in tax:
              parts = [p.strip() for p in tax.split(";") if p.strip()]
              if parts:
                  sp = parts[-1]
                  # normalize underscores to spaces, strip brackets/quotes
                  sp = re.sub(r"[_\"'\[\]]", " ", sp).strip()
                  return sp if sp else "Unassigned"
          # Handle GTDB-style ranks (s__/g__)
          m = re.search(r"s__([^;]+)", tax)
          if m:
              return m.group(1).replace("_", " ").strip()
          m = re.search(r"g__([^;]+)", tax)
          if m:
              return (m.group(1).replace("_", " ").strip() + " sp.")
          toks = tax.split()
          if len(toks) >= 2:
              return (toks[-2] + " " + toks[-1]).replace("_"," ").strip()
          return "Unassigned"

      pafs = sorted(glob.glob(os.path.join(paf_dir, "*.paf")))
      if not pafs:
          sys.exit("No PAF files found. Did mapping run?")
      sample2counts = {{}}
      for paf in pafs:
          sid = os.path.basename(paf)[:-4]
          cnt = Counter()
          with open(paf) as fh:
              for line in fh:
                  if not line or line.startswith("#"): continue
                  cols = line.rstrip("\n").split("\t")
                  # PAF core columns (1-based): 6=tname, 11=aln block length, 12=mapq
                  try:
                      tname = cols[5]
                      aln   = int(cols[10])
                      mapq  = int(cols[11])
                  except Exception:
                      continue
                  if mapq < mapq_min or aln < aln_min_bp:
                      continue
                  tax = id2tax.get(tname, "NA")
                  sp  = species_from_tax(tax)
                  cnt[sp] += 1
          out_tsv = os.path.join(r"{OUT}/silva/species", f"silva_species_{{sid}}.tsv")
          with open(out_tsv, "w", newline="") as fh:
              w = csv.writer(fh, delimiter="\t")
              w.writerow(["Species", sid])
              for sp, c in cnt.most_common():
                  w.writerow([sp, c])
          sample2counts[sid] = cnt

      all_species = sorted(set().union(*[set(c) for c in sample2counts.values()])) if sample2counts else []
      samples = sorted(sample2counts.keys())
      with open(out_merged, "w", newline="") as fh:
          w = csv.writer(fh, delimiter="\t")
          w.writerow(["Species"] + samples)
          for sp in all_species:
              w.writerow([sp] + [sample2counts[s].get(sp, 0) for s in samples])
      PY
    """

rule isonclust3:
    input: rules.fastcat_filter.output.fastq
    output: directory(os.path.join(TMP, "OTUs"))
    threads: Rq("isonclust3", "threads")
    resources:
        mem_mb   = Rq("isonclust3", "mem_mb"), 
        runtime  = Rq("isonclust3", "runtime"),
        partition= Rq("isonclust3", "partition"),
        account  = Rq("isonclust3", "account"),
        extra    =  R("isonclust3", "extra") 
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
    threads: Rq("spoa_consensus", "threads")
    resources:
        mem_mb   = Rq("spoa_consensus", "mem_mb"), 
        runtime  = Rq("spoa_consensus", "runtime"),
        partition= Rq("spoa_consensus", "partition"),
        account  = Rq("spoa_consensus", "account"),
        extra    =  R("spoa_consensus", "extra") 
    params:
        max_reads = int(config.get("spoa_max_reads", 500)),
        min_reads = int(config.get("spoa_min_reads", 3)),
        extra     = lambda wc: config.get("spoa_extra","")
    log: os.path.join(OUT, "logs/spoa_consensus.log")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {output}
      find {input} -type f -path "*/clustering/fastq_files/*.fastq" | while read -r fq; do
        sample=$(basename "$(dirname "$(dirname "$fq")")")
        cid=$(basename "$fq" .fastq)
        out="{output}/${{sample}}_${{cid}}.fasta"
        n=$(awk 'END{{print NR/4}}' "$fq")
        if (( n < {params.min_reads} )); then continue; fi
        tmpd=$(mktemp -d)
        tmpf="$tmpd/reads.fastq"  
        if (( n > {params.max_reads} )); then
          awk -v m={params.max_reads} 'NR%4==1{{c++}} c<=m{{print}}' "$fq" > "$tmpf"
        else
          cp "$fq" "$tmpf"
        fi
        spoa {params.extra} "$tmpf" > "$out"
        rm -rf "$tmpd"
      done
    """
    
rule vsearch_pool_cluster:
    input: rules.spoa_consensus.output
    output:
        drafts = os.path.join(TMP, "pooled/all_draft_otus.fasta"),
        cent99 = os.path.join(OUT, "otu/otus_centroids_99.fasta"),
        cent97 = os.path.join(OUT, "otu/otus_centroids_97.fasta")
    threads: Rq("vsearch_pool_cluster", "threads")
    resources:
        mem_mb   = Rq("vsearch_pool_cluster", "mem_mb"), 
        runtime  = Rq("vsearch_pool_cluster", "runtime"),
        partition= Rq("vsearch_pool_cluster", "partition"),
        account  = Rq("vsearch_pool_cluster", "account"),
        extra    =  R("vsearch_pool_cluster", "extra") 
    params:
        id_primary = lambda wc: float(config["otu_id_primary"]),   
        id_legacy  = lambda wc: float(config["otu_id_legacy"]),    
        min_unique = lambda wc: int(config["min_unique_size"])     
    log: os.path.join(OUT, "logs/vsearch_pool_cluster.log")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p "$(dirname {output.drafts})" "$(dirname {output.cent99})" "{TMP}/pooled"
      cat {input}/*.fasta > {output.drafts}

      vsearch --derep_fulllength {output.drafts} --sizeout --relabel OTU_ --strand both \
              --minuniquesize {params.min_unique} --threads {threads} \
              --output {TMP}/pooled/otus_derep.fasta

      vsearch --cluster_fast {TMP}/pooled/otus_derep.fasta --id {params.id_primary} \
              --centroids {output.cent99} --threads {threads}

      vsearch --cluster_fast {TMP}/pooled/otus_derep.fasta --id {params.id_legacy} \
              --centroids {output.cent97} --threads {threads}
    """

rule map_all_reads:
    input:
        reads = os.path.join(TMP, "filtered"),
        refs  = os.path.join(OUT, "otu/otus_centroids_99.fasta")
    output:
        all_reads = os.path.join(TMP, "polished/all_reads.fastq"),
        bam       = os.path.join(TMP, "polished/map_r0.bam")
    threads: Rq("map_all_reads", "threads")
    resources:
        mem_mb   = Rq("map_all_reads", "mem_mb"), 
        runtime  = Rq("map_all_reads", "runtime"),
        partition= Rq("map_all_reads", "partition"),
        account  = Rq("map_all_reads", "account"),
        extra    =  R("map_all_reads", "extra") 
    log: os.path.join(OUT, "logs/map_all_reads.log")
    container: CONTAINERS["nanoalign"]
    shell: r"""
      set -euo pipefail

      mkdir -p "$(dirname {output.all_reads})" "$(dirname {output.bam})"

      : > "{output.all_reads}"
      find "{input.reads}" -maxdepth 1 -type f -name '*.fastq' -print0 \
        | xargs -0 cat >> "{output.all_reads}"

      minimap2 -t {threads} -ax map-ont "{input.refs}" "{output.all_reads}" \
        | samtools sort -@ {threads} -m 2G -o "{output.bam}"

      samtools index "{output.bam}"
    """
    
# racon_round1
rule racon_round1:
    input: reads = ALL_READS_FQ, bam = MAP_BAM_R0, refs = OTU_CENTROIDS_FASTA
    output: r1 = R1_FASTA
    threads: Rq("racon_round1", "threads")
    resources:
        mem_mb   = Rq("racon_round1", "mem_mb"), 
        runtime  = Rq("racon_round1", "runtime"),
        partition= Rq("racon_round1", "partition"),
        account  = Rq("racon_round1", "account"),
        extra    =  R("racon_round1", "extra") 
    container: CONTAINERS["nanoalign"]
    shell: r"""
        set -euo pipefail
        echo "== Versions ==" >&2
        samtools --version | head -n1 >&2 || true
        racon --version >&2 || true
        echo "reads: {input.reads}" >&2
        echo "bam:   {input.bam}"   >&2
        echo "refs:  {input.refs}"  >&2
        echo "threads: {threads}"   >&2

        tmp_sam=$(mktemp --suffix=.sam)
        trap 'rm -f "$tmp_sam"' EXIT

        samtools view -@ {threads} -h "{input.bam}" -o "$tmp_sam"

        racon -t {threads} "{input.reads}" "$tmp_sam" "{input.refs}" > "{output.r1}"

        test -s "{output.r1}" && grep -q "^>" "{output.r1}"
    """
    
rule map_r1:
    input: reads = ALL_READS_FQ, r1 = R1_FASTA
    output: bam = MAP_BAM_R1
    threads: Rq("map_r1", "threads")
    resources:
        mem_mb   = Rq("map_r1", "mem_mb"), 
        runtime  = Rq("map_r1", "runtime"),
        partition= Rq("map_r1", "partition"),
        account  = Rq("map_r1", "account"),
        extra    =  R("map_r1", "extra") 
    container: CONTAINERS["nanoalign"]
    shell: r"""
        set -euo pipefail
        tmpdir="{resources.tmpdir}"
        if [[ -z "$tmpdir" || "$tmpdir" == "TBD" || "$tmpdir" == "<TBD>" ]]; then
          tmpdir="/tmp"
        fi
        mkdir -p "$tmpdir"
      
        minimap2 -t {threads} -ax map-ont "{input.r1}" "{input.reads}" \
          | samtools sort -@ {threads} -m 1G -T "$tmpdir/map_r1" -o "{output.bam}"
        samtools index -@ {threads} "{output.bam}"
      """
    
# racon_round2
rule racon_round2:
    input: reads = ALL_READS_FQ, bam = MAP_BAM_R1, r1 = R1_FASTA
    output: r2 = R2_FASTA
    threads: Rq("racon_round2", "threads")
    resources:
        mem_mb   = Rq("racon_round2", "mem_mb"), 
        runtime  = Rq("racon_round2", "runtime"),
        partition= Rq("racon_round2", "partition"),
        account  = Rq("racon_round2", "account"),
        extra    =  R("racon_round2", "extra") 
    container: CONTAINERS["nanoalign"]
    shell: r"""
        set -euo pipefail
        tmp_sam=$(mktemp --suffix=.sam)
        trap 'rm -f "$tmp_sam"' EXIT

        samtools view -@ {threads} -h "{input.bam}" -o "$tmp_sam"
        racon -t {threads} "{input.reads}" "$tmp_sam" "{input.r1}" > "{output.r2}"

        test -s "{output.r2}" && grep -q "^>" "{output.r2}"
    """
    
rule medaka_polish:
    input:
        reads = ALL_READS_FQ,
        draft = R2_FASTA
    output:
        polished = POLISHED
    threads: Rq("medaka_polish", "threads")
    resources:
        mem_mb   = Rq("medaka_polish", "mem_mb"), 
        runtime  = Rq("medaka_polish", "runtime"),
        partition= Rq("medaka_polish", "partition"),
        account  = Rq("medaka_polish", "account"),
        extra    =  R("medaka_polish", "extra") 
    params:
        medaka_model = lambda wc: config["medaka_model"]
    container:
        CONTAINERS["gpu"]
    shell:
        r"""
        set -euo pipefail
        export OMP_NUM_THREADS=1
        mkdir -p "{POLISH_DIR}/medaka_refined"

        medaka_consensus \
          -i "{input.reads}" \
          -d "{input.draft}" \
          -o "{POLISH_DIR}/medaka_refined" \
          -m "{params.medaka_model}" \
          -t {threads} \
          --bacteria

        cp "{POLISH_DIR}/medaka_refined/consensus.fasta" "{output.polished}"
        """

##############################################
#  POST-POLISH OTU PROCESSING STAGE
#  (Runs after medaka_polish)
##############################################

# 1. Identify chimeras + assign taxonomy
rule chimera_taxonomy:
    input:
        fasta = POLISHED,
        db    = ITGDB_UDB
    output:
        nonchim = os.path.join(OUT, "otu/otus_clean.fasta"),
        chimera = os.path.join(OUT, "otu/otus_chimeras.fasta"),
        sintax  = os.path.join(OUT, "otu/otus_taxonomy.sintax")
    threads: Rq("chimera_taxonomy", "threads")
    resources:
        mem_mb    = Rq("chimera_taxonomy", "mem_mb"),
        runtime   = Rq("chimera_taxonomy", "runtime"),
        partition = Rq("chimera_taxonomy", "partition"),
        account   = Rq("chimera_taxonomy", "account"),
        extra     = R("chimera_taxonomy", "extra")
    container: CONTAINERS["cpu"]
    params:
        sintax_cutoff = lambda wc: SINTAX_CUTOFF
    shell: r"""
      set -euo pipefail
      command -v vsearch >/dev/null || {{ echo "vsearch not found"; exit 127; }}

      mkdir -p {OUT}/otu

      vsearch --uchime_denovo {input.fasta} \
        --nonchimeras {output.nonchim} \
        --chimeras {output.chimera} \
        --threads {threads}

      vsearch --sintax {output.nonchim} \
              --db {input.db} \
              --sintax_cutoff {params.sintax_cutoff} \
              --tabbedout {output.sintax} \
              --threads {threads}
    """


# 2. Multiple sequence alignment of non-chimeric OTUs
rule otu_alignment:
    input:
        fasta = rules.chimera_taxonomy.output.nonchim
    output:
        msa = os.path.join(OUT, "otu/otu_references_aligned.fasta")
    threads: Rq("otu_alignment", "threads")
    resources:
        mem_mb    = Rq("otu_alignment", "mem_mb"),
        runtime   = Rq("otu_alignment", "runtime"),
        partition = Rq("otu_alignment", "partition"),
        account   = Rq("otu_alignment", "account"),
        extra     = R("otu_alignment", "extra")
    container: CONTAINERS["mafft"]
    shell: r"""
      set -euo pipefail
      command -v mafft >/dev/null || {{ echo "mafft not found"; exit 127; }}
      mafft --auto --thread {threads} "{input.fasta}" > "{output.msa}"
    """


# 3. Phylogenetic tree inference
rule iqtree3_tree:
    input:
        msa = rules.otu_alignment.output.msa
    output:
        tree = os.path.join(OUT, "otu/otu_tree.treefile")
    threads: Rq("iqtree3_tree", "threads")
    resources:
        mem_mb    = Rq("iqtree3_tree", "mem_mb"),
        runtime   = Rq("iqtree3_tree", "runtime"),
        partition = Rq("iqtree3_tree", "partition"),
        account   = Rq("iqtree3_tree", "account"),
        extra     = R("iqtree3_tree", "extra")
    log: os.path.join(OUT, "logs/iqtree3_tree.log")
    container: CONTAINERS["iqtree3"]
    shell: r"""
      set -euo pipefail

      # Ensure output/log dirs exist
      mkdir -p "{OUT}/otu" "$(dirname "{log}")"

      # Find the iqtree binary inside the container
      IQ=$(command -v iqtree || command -v iqtree3 || command -v iqtree2 || true)
      if [ -z "$IQ" ]; then
        echo "No iqtree executable found in container PATH." >&2
        exit 127
      fi

      # Run IQ-TREE
      "$IQ" -s "{input.msa}" -nt {threads} -m TEST -bb 1000 -alrt 1000 \
            -pre "{OUT}/otu/otu_tree" > "{log}" 2>&1

      # Sanity check expected output
      test -s "{output.tree}"
    """
    
# 4. Per-sample read counts for each OTU
rule otu_table_per_sample:
    input:
        refs  = rules.chimera_taxonomy.output.nonchim,
        reads = rules.fastcat_filter.output.fastq
    output:
        merged = os.path.join(OUT, "otu/otu_table_merged.tsv")
    threads: Rq("otu_table_per_sample", "threads")
    resources:
        mem_mb    = Rq("otu_table_per_sample", "mem_mb"),
        runtime   = Rq("otu_table_per_sample", "runtime"),
        partition = Rq("otu_table_per_sample", "partition"),
        account   = Rq("otu_table_per_sample", "account"),
        extra     = R("otu_table_per_sample", "extra")
    params:
        map_id = lambda wc: MAP_ID,
        strand = lambda wc: STRAND
    container: CONTAINERS["cpu"]
    shell:
      r"""
      set -euo pipefail
      mkdir -p {OUT}/otu/tables
      shopt -s nullglob

      PYBIN=$(command -v python || command -v python3 || true)
      if [ -z "$PYBIN" ]; then
        echo "No python interpreter found in container." >&2
        exit 127
      fi

      for fq in {input.reads}/*.fastq; do
        sid=$(basename "$fq" .fastq)
        vsearch --usearch_global "$fq" \
                --db {input.refs} \
                --id {params.map_id} \
                --strand {params.strand} \
                --otutabout {OUT}/otu/tables/otu_table_${{sid}}.tsv \
                --threads {threads}
      done

      "$PYBIN" - <<'PY'
      import glob, os, csv, sys
      out = r"{output.merged}"
      tables = sorted(glob.glob(os.path.join(r"{OUT}", "otu", "tables", "otu_table_*.tsv")))
      if not tables:
          sys.exit("No per-sample OTU tables found to merge.")

      merged = {{}}
      samples = []
      headers = ["OTU"]
      
      for t in tables:
          # sample id from filename
          sid = os.path.basename(t)
          if sid.startswith("otu_table_"):
              sid = sid[len("otu_table_"):]
          if sid.endswith(".tsv"):
              sid = sid[:-4]
          samples.append(sid)
      
          try:
              with open(t, newline="") as fh:
                  r = csv.reader(fh, delimiter="\t")
                  try:
                      hdr = next(r)      # skip header if present
                  except StopIteration:
                      # empty file -> skip
                      sys.stderr.write(f"WARNING: empty table skipped: {{t}}\\n")
                      continue
                  rows = 0
                  for row in r:
                      if not row:
                          continue
                      rows += 1
                      otu = row[0]
                      val = row[1] if len(row) > 1 and row[1] != "" else "0"
                      merged.setdefault(otu, {{}})[sid] = val
                  if rows == 0:
                      sys.stderr.write(f"WARNING: header-only table skipped: {{t}}\\n")
          except Exception as e:
              sys.stderr.write(f"WARNING: failed to parse {{t}}: {{e}}\\n")
              continue
      
      with open(out, "w", newline="") as fh:
          w = csv.writer(fh, delimiter="\t")
          w.writerow(["OTU"] + samples)
          for otu in sorted(merged.keys()):
              w.writerow([otu] + [merged.get(otu, {{}}).get(s, "0") for s in samples])
      PY
      """    
    
