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
for p in (OUT, TMP, os.path.join(OUT,"otu"), os.path.join(OUT,"logs"), os.path.join(OUT,"benchmarks")):
    Path(p).mkdir(parents=True, exist_ok=True)

# Containers
CONTAINERS = {
    "cpu":       _expand(config.get("container_cpu",       "$PROJ_ROOT/containers/nanomb.sif")),
    "gpu":       _expand(config.get("container_gpu",       "$PROJ_ROOT/containers/nanombgpu.sif")),
    "nanoalign": _expand(config.get("container_nanoalign", "$PROJ_ROOT/containers/nanoalign.sif")),
    "dorado":    _expand(config.get("container_dorado",    "$PROJ_ROOT/containers/dorado.sif")),
}

# --- Nextflow wf-16s integration (optional) ---

WF16S_ENABLE = bool(config.get("wf16s", {}).get("enable", False))
WF16S_OUTDIR = config.get("wf16s", {}).get("out_dir") or os.path.join(OUT, "wf16s")
WF16S_WORK   = config.get("wf16s", {}).get("work_dir") or os.path.join(TMP, "wf16s_work")

CONTAINERS["nextflow"] = _expand(config.get("container_nextflow", "")) or CONTAINERS["cpu"]


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

BASECALL_BAM_T      = os.path.join(BASECALL_DIR_T, "{run}.bam")
BASECALL_SUMMARY_T  = os.path.join(SUMMARY_DIR_T, "basecall", "{run}_basecall_summary.tsv")

DEMUX_SUMMARY_T     = os.path.join(SUMMARY_DIR_T, "demux", "{run}_barcoding_summary.txt")
DEMUX_DONE_T        = os.path.join(DEMUX_DIR_T, ".done")

ITGDB_UDB   = _expand(config["itgdb"]["sintax_udb"])
ITGDB_FASTA = _expand(config["itgdb"]["seq_fasta"])     
ITGDB_TAX   = _expand(config["itgdb"]["tax_tsv"])

MAP_ID  = float(config.get("map_id", 0.98))
COLLAPSE_ID  = float(config.get("collapse_id", 0.997))
STRAND  = config.get("strand", "both")
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
        os.path.join(OUT, "itgdb/species/itgdb_species_merged.tsv"),  
        os.path.join(TMP, "wf16s_in", ".staged.ok"),
      ]
)

if WF16S_ENABLE:
    _final_targets = tuple(_final_targets) + (os.path.join(WF16S_OUTDIR, ".wf16s.done"),)

rule all:
    input: _final_targets

rule preflight:
    input: db = ITGDB_UDB
    output: touch(os.path.join(TMP, "preflight.ok"))
    container: CONTAINERS["cpu"]
    run:
        if not input.db or not os.path.exists(input.db) or os.path.getsize(input.db) == 0:
            raise WorkflowError(f"SINTAX DB missing/empty: {input.db!r}")

        if not ITGDB_FASTA or not os.path.exists(ITGDB_FASTA) or os.path.getsize(ITGDB_FASTA) == 0:
            raise WorkflowError(f"itgdb reference missing/empty: {ITGDB_FASTA!r}")

        must_have = ("cpu","gpu","nanoalign","dorado")
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
        bam     = BASECALL_BAM_T,
        summary = BASECALL_SUMMARY_T
    threads: Rq("dorado_basecall", "threads")
    resources:
        mem_mb         = Rq("dorado_basecall", "mem_mb"),
        runtime        = Rq("dorado_basecall", "runtime"),
        slurm_partition = Rq("dorado_basecall", "slurm_partition"),
        slurm_account  = Rq("dorado_basecall", "slurm_account"),
        slurm_extra    = R("dorado_basecall", "slurm_extra", "")       
    params:
        modelname = lambda wc: config.get("dorado_model_name","sup"),
        modelsdir = lambda wc: _expand(config.get("dorado_models_dir","/models")),
        extra     = lambda wc: config.get("dorado_extra",""),
        container_rev = lambda wc: config["container_rev"].get("dorado","0")
    log: LOG_BASECALL_T                        
    container: CONTAINERS["dorado"]
    shell: r"""
      set -euo pipefail
      mkdir -p "$(dirname "{output.bam}")" "$(dirname "{output.summary}")"
      
       if [[ -n "{params.modelsdir}" && -n "{params.modelname}" ]]; then
         dorado download --models-directory "{params.modelsdir}" --model "{params.modelname}" >/dev/null || true
       fi
      model_arg="{params.modelname}"
      if [[ -n "{params.modelsdir}" && -d "{params.modelsdir}/{params.modelname}" ]]; then
        model_arg="{params.modelsdir}/{params.modelname}"
      fi
       
      dorado basecaller "$model_arg" "{input.pod5}" \
        --device cuda:0 --recursive --no-trim \
        $([[ -n "{params.modelsdir}" ]] && printf -- "--models-directory %q " "{params.modelsdir}") \
        {params.extra} > "{output.bam}"

      dorado summary "{output.bam}" > "{output.summary}"
    """

rule dorado_demux:
    input:
        basecalled = BASECALL_BAM_T            
    output:
        demuxed   = directory(DEMUX_DIR_T),
        summary = DEMUX_SUMMARY_T,
        done    = DEMUX_DONE_T
    threads: Rq("dorado_demux", "threads")
    resources:
        mem_mb         = Rq("dorado_demux", "mem_mb"),
        runtime        = Rq("dorado_demux", "runtime"),
        slurm_partition = Rq("dorado_demux", "slurm_partition"),
        slurm_account  = Rq("dorado_demux", "slurm_account"),
        slurm_extra    =  R("dorado_demux", "slurm_extra", "")          
    params:
        sheet_pat  = lambda wc: esc_braces((_expand(config.get("sample_sheet_pattern","")) or "").replace("{run}", "___RUN___")),
        sheet_name = lambda wc: esc_braces(SHEET_NAME.replace("{run}", "___RUN___")),
        sheet_dir  = lambda wc: esc_braces(SHEET_DIR),
        kit        = lambda wc: config.get("barcode_kit",""),
        container_rev = lambda wc: config["container_rev"].get("dorado","0")
    container: CONTAINERS["dorado"]
    shell: r"""
      set -euo pipefail
      mkdir -p "{output.demuxed}" "$(dirname "{output.summary}")"

      ssp="{params.sheet_pat}"
      run_id="{wildcards.run}"
      if [[ -n "$ssp" ]]; then
        ssp="$(printf '%s' "$ssp" | sed "s/___RUN___/$run_id/g")"
      else
        sname_tmpl="{params.sheet_name}"
        sname="$(printf '%s' "$sname_tmpl" | sed "s/___RUN___/$run_id/g")"
        ssp="{params.sheet_dir}/$sname"
      fi
      [[ -r "$ssp" ]] || ssp=""

      dorado demux "{input.basecalled}" --output-dir "{output.demuxed}" \
        $([[ -n "$ssp" ]] && printf -- "--sample-sheet %q " "$ssp") \
        $([[ -n "{params.kit}" ]] && printf -- "--kit-name %q " "{params.kit}") \
        --emit-summary

      if compgen -G "{output.demuxed}"/*.txt >/dev/null; then
        mv -f "{output.demuxed}"/*.txt "{output.summary}"
      fi
      : > "{output.done}"
    """
    

rule dorado_all_runs:
    input:
        [BASECALL_BAM_T.format(run=r) for r in RUNS],
        [DEMUX_DONE_T.format(run=r)   for r in RUNS]
    output:
        touch(os.path.join(TMP, "dorado_all_runs.ok"))
    shell: "true"
    
def demux_index_path_for_run(run): return os.path.join(TMP, f"demux_index_{run}.tsv")

# --- make this a checkpoint so we can discover samples at runtime ---
checkpoint demux_index_one_run:
    input:
        demux_dir = DEMUX_DIR_T,
        ok        = DEMUX_DONE_T
    output:
        tsv = DEMUX_INDEX_T
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      out="{output.tsv}"
      mkdir -p "$(dirname "$out")"
      printf "sample\trun\tbam\n" > "$out"
      shopt -s nullglob
      for bam in "{input.demux_dir}"/*.bam; do
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

# --- helper functions that resolve AFTER the checkpoint completes ---
def _samples_for_run_after_demux(wc):
    import csv as _csv
    ck  = checkpoints.demux_index_one_run.get(run=wc.run)
    idx = ck.output.tsv
    samples = []
    with open(idx) as fh:
        rdr = _csv.DictReader(fh, delimiter="\t")
        for row in rdr:
            if row["run"] == wc.run:
                samples.append(row["sample"])
    if not samples:
        raise WorkflowError(f"No samples listed for run={wc.run} in {idx}")
    return sorted(set(samples))

def trim_fastqs_for_run(wc):
    return expand(os.path.join(raw_dir_for_run(wc.run), "{sample}.fastq"),
                  sample=_samples_for_run_after_demux(wc))

def bam_for_sample_run(wc):
    import csv as _csv
    ck = checkpoints.demux_index_one_run.get(run=wc.run)
    idx = ck.output.tsv
    wanted = []
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
        mem_mb         = Rq("dorado_trim", "mem_mb"),
        runtime        = Rq("dorado_trim", "runtime"),
        slurm_partition = Rq("dorado_trim", "slurm_partition"),
        slurm_account  = Rq("dorado_trim", "slurm_account"),
        slurm_extra    = R("dorado_trim", "slurm_extra", "")
    params:
        kit          = lambda wc: config.get("barcode_kit",""),
        minlen       = lambda wc: int(config.get("trim_minlen", 0)),
        skip_samples = lambda wc: " ".join(config.get("trim_skip_glob", [])) or "__NONE__",
        container_rev = lambda wc: config["container_rev"].get("dorado","0")
    log: LOG_TRIM_T
    container: CONTAINERS["dorado"]
    shell: r"""
      set -euo pipefail
      mkdir -p "$(dirname "{output.fastq}")"

      sid="{wildcards.sample}"
      set -f
      for pat in {params.skip_samples}; do
        [[ "$pat" == "__NONE__" ]] && break
        case "$sid" in $pat)
          echo "skip {wildcards.run}/$sid" >&2
          : > "{output.fastq}"
          exit 0
        ;;
        esac
      done
      set +f

      tmp="$(mktemp "$(dirname "{output.fastq}")/.trim.XXXXXX")"
      if ! dorado trim \
           $([[ -n "{params.kit}" ]] && printf -- "--sequencing-kit %q " "{params.kit}") \
           --emit-fastq \
           "{input.bam}" > "$tmp" 2>> "{log}"; then
        echo "[dorado_trim] dorado trim failed, see {log}" >&2
        rm -f "$tmp"
        exit 1
      fi

      if [[ {params.minlen} -gt 0 ]]; then
        awk 'BEGIN{{OFS="\n"}} NR%4==1{{h=$0}} NR%4==2{{s=$0}} NR%4==3{{p=$0}} NR%4==0{{q=$0; if(length(s)>={params.minlen}) print h,s,p,q}}' \
          "$tmp" > "{output.fastq}"
        rm -f "$tmp"
      else
        mv -f "$tmp" "{output.fastq}"
      fi
    """    
    
rule dorado_trim_run_done:
    input: fastqs = trim_fastqs_for_run
    output: touch(os.path.join(RAW_DIR_T, "demux_trim.done"))
    shell: "true"


# ---------------- QC & prep (CPU containers) ----------------

from snakemake.io import Wildcards

def all_trimmed_fastqs(wc):
    from pathlib import Path
    files = []
    for run in RUNS:
        _ = checkpoints.demux_index_one_run.get(run=run)
        d = Path(raw_dir_for_run(run))
        if d.exists():
            files += [str(p) for p in d.glob("*.fastq") if p.stat().st_size > 0]
    return sorted(set(files))

rule fastcat_filter:
    input:
        fastqs = all_trimmed_fastqs,
        deps   = [os.path.join(raw_dir_for_run(r), "demux_trim.done") for r in RUNS]
    output:
        fastq   = directory(os.path.join(TMP, "filtered")),
        filesum = os.path.join(OUT, "qc", "fastcat_file_summary.tsv"),
        readsum = os.path.join(OUT, "qc", "fastcat_read_summary.tsv"),
        done    = touch(os.path.join(TMP, "filtered", ".fastcat_filter.done"))
    threads: Rq("fastcat_filter", "threads")
    resources:
        mem_mb   = Rq("fastcat_filter", "mem_mb"),
        runtime  = Rq("fastcat_filter", "runtime"),
        partition = Rq("fastcat_filter", "partition"),
        account  = Rq("fastcat_filter", "account"),
        extra    = R("fastcat_filter", "extra")
    params:
        outdir_base = OUT,
        container_rev = lambda wc: config["container_rev"].get("cpu","0"),
        min_q   = lambda wc: config["min_qscore"],
        minlen  = lambda wc: config["minlength"],
        maxlen  = lambda wc: config["maxlength"],
        histdir = lambda wc: os.path.join(OUT, "qc", "fastcat-histograms"),
        exclude = lambda wc: " ".join(config.get("trim_skip_glob", [])) or "__NONE__"
    log: os.path.join(OUT, "logs/fastcat_filter.log")
    benchmark: os.path.join(OUT, "benchmarks/fastcat_filter.tsv")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      shopt -s nullglob
      mkdir -p {output.fastq} {params.histdir} "$(dirname "{output.filesum}")"

      for fq in {input.fastqs}; do
        [[ -s "$fq" ]] || continue
        run=$(basename "$(dirname "$fq")")
        base=$(basename "$fq")

        for pat in {params.exclude}; do
          [[ "$pat" == "__NONE__" ]] && break
          case "$base" in $pat) continue 2;; 
          esac
        done

        stem=$(printf "%s" "$base" | sed -E 's/\.fastq(\.gz)?$//; s/\.fq(\.gz)?$//')
        stem="${{run}}__${{stem}}"
        hdir="{params.histdir}/${{stem}}"
        filesum_part="{params.outdir_base}/qc/fastcat_file_summary_${{stem}}.tsv"
        readsum_part="{params.outdir_base}/qc/fastcat_read_summary_${{stem}}.tsv"
        out="{output.fastq}/${{stem}}.fastq"

        rm -rf "$hdir"

        if [[ "$fq" == *.gz ]]; then
          zcat "$fq" | fastcat --dust --min_qscore {params.min_q} --min_length {params.minlen} --max_length {params.maxlen} \
            --histograms "$hdir" --file "$filesum_part" --read "$readsum_part" - > "$out"
        else
          fastcat --dust --min_qscore {params.min_q} --min_length {params.minlen} --max_length {params.maxlen} \
            --histograms "$hdir" --file "$filesum_part" --read "$readsum_part" "$fq" > "$out"
        fi
      done

      if ls {params.outdir_base}/qc/fastcat_file_summary_*.tsv >/dev/null 2>&1; then
        awk 'FNR==1 && NR!=1 {{ next }} 1' {params.outdir_base}/qc/fastcat_file_summary_*.tsv > {output.filesum}
        awk 'FNR==1 && NR!=1 {{ next }} 1' {params.outdir_base}/qc/fastcat_read_summary_*.tsv  > {output.readsum}
      else
        : > {output.filesum}
        : > {output.readsum}
      fi
    """
    
rule nanoplot_qc:
    input:
        filt_dir = rules.fastcat_filter.output.fastq,
        filesum  = rules.fastcat_filter.output.filesum,
        readsum  = rules.fastcat_filter.output.readsum
    output:
        directory(os.path.join(OUT, "qc/nanoplot"))
    threads: Rq("nanoplot_qc", "threads")
    resources:
        mem_mb   = Rq("nanoplot_qc", "mem_mb"),
        runtime  = Rq("nanoplot_qc", "runtime"),
        partition = Rq("nanoplot_qc", "partition"),
        account  = Rq("nanoplot_qc", "account"),
        extra    = R("nanoplot_qc", "extra")
    params:
        maxlen = lambda wc: config["maxlength"],
        minlen = lambda wc: config["minlength"],
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {output}
      cat {input.filt_dir}/*.fastq > {output}/all.fastq
      NanoPlot --fastq {output}/all.fastq -o {output} --drop_outliers \
        --maxlength {params.maxlen} --minlength {params.minlen}
    """

# ---------------- wf-16s branch --------------

rule stage_wf16s_input:
    input:
        filt_done = rules.fastcat_filter.output.done,
        filt_dir  = rules.fastcat_filter.output.fastq
    output:
        indir    = directory(os.path.join(TMP, "wf16s_in")),
        done     = touch(os.path.join(TMP, "wf16s_in", ".staged.ok"))
    threads: Rq("stage_wf16s_input", "threads")
    resources:
        mem_mb   = Rq("stage_wf16s_input", "mem_mb"), 
        runtime  = Rq("stage_wf16s_input", "runtime"),
        partition = Rq("stage_wf16s_input", "partition"),
        account  = Rq("stage_wf16s_input", "account"),
        extra    =  R("stage_wf16s_input", "extra") 
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      in_dir="{input.filt_dir}"
      out_dir="{output.indir}"
      mkdir -p "$out_dir"

      shopt -s nullglob
      for fq in "$in_dir"/*.fastq "$in_dir"/*.fastq.gz; do
        [[ -s "$fq" ]] || continue
        base="$(basename "$fq")"
        stem="$(printf "%s" "$base" | sed -E 's/\.fastq(\.gz)?$//')"
        sid="$(printf "%s" "$stem" | sed -E 's/^.*_//')"

        sdir="$out_dir/$sid"
        mkdir -p "$sdir"
        ln -sf "$fq" "$sdir/$base"
      done

      echo "[stage_wf16s_input] sample dirs: $(find "$out_dir" -mindepth 1 -maxdepth 1 -type d | wc -l)" >&2
      find "$out_dir" -mindepth 1 -maxdepth 1 -type d | sort | head -n 8 >&2
    """
    
rule wf16s_run:
    input:
        staged = rules.stage_wf16s_input.output.done,
        indir  = rules.stage_wf16s_input.output.indir,
    output:
        done   = touch(os.path.join(WF16S_OUTDIR, ".wf16s.done"))
    threads:  Rq("wf16s_run", "threads")
    resources:
        mem_mb   = Rq("wf16s_run", "mem_mb"),
        runtime  = Rq("wf16s_run", "runtime"),
        partition = Rq("wf16s_run", "partition"),
        account  = Rq("wf16s_run", "account"),
        extra    = R("wf16s_run", "extra"),
    params:
        min_q   = lambda wc: config["min_qscore"],
        minlen  = lambda wc: config["minlength"],
        maxlen  = lambda wc: config["maxlength"],
        repo    = lambda wc: config["wf16s"].get("repo","epi2me-labs/wf-16s"),
        profile = lambda wc: config["wf16s"].get("profile","auto"),
        rev     = lambda wc: config["wf16s"].get("rev", ""),    
        extra   = lambda wc: config["wf16s"].get("extra_args",""),
        outdir   = WF16S_OUTDIR,
        workdir  = WF16S_WORK,
    shell: r"""
      set -euo pipefail
      mkdir -p "{params.outdir}" "{params.workdir}"
      
      if ! command -v nextflow >/dev/null 2>&1; then
        module load nextflow >/dev/null 2>&1 || true
      fi

      nf_profile="{params.profile}"
      if [ "$nf_profile" = "auto" ]; then
        if command -v apptainer >/dev/null 2>&1; then
          nf_profile="apptainer"
        elif command -v singularity >/dev/null 2>&1; then
          nf_profile="singularity"
        elif command -v docker >/dev/null 2>&1; then
          nf_profile="docker"
        else
          nf_profile="conda"
        fi
      fi

      echo "[wf16s_run] using per-sample dir layout under: {input.indir}" >&2
      find "{input.indir}" -mindepth 1 -maxdepth 1 -type d | sort | head -n 8 >&2
      
      nf_cmd=( nextflow run "{params.repo}"
               --fastq "{input.indir}"
               --min_len "{params.minlen}"
               --min_read_qual "{params.min_q}"
               --max_len "{params.maxlen}"
               --threads "{threads}"
               -profile "$nf_profile"
               --out_dir "{params.outdir}"
               {params.extra}
               -work-dir "{params.workdir}"
               -resume )

      if [ -n "{params.rev}" ]; then
        nf_cmd+=( -r "{params.rev}" )
      fi

      echo "=== wf-16s ===" >&2
      printf "%q " "${{nf_cmd[@]}}" >&2; echo >&2

      "${{nf_cmd[@]}}"

      : > "{output.done}"

      
    """
    
# ---------------- OTU branch ----------------
rule itgdb_index:
    input:  fasta = ITGDB_FASTA
    output: mmi   = os.path.join(OUT, "itgdb/index", "ITGDB_16S.mmi")
    threads: Rq("itgdb_index", "threads")
    resources:
        mem_mb   = Rq("itgdb_index", "mem_mb"), 
        runtime  = Rq("itgdb_index", "runtime"),
        partition = Rq("itgdb_index", "partition"),
        account  = Rq("itgdb_index", "account"),
        extra    =  R("itgdb_index", "extra"),
    params:
        container_rev = lambda wc: config["container_rev"].get("nanoalign","0")
    container: CONTAINERS["nanoalign"]
    shell: r"""
      set -euo pipefail
      mkdir -p "$(dirname "{output.mmi}")"
      minimap2 -d "{output.mmi}" "{input.fasta}"
    """
    
rule itgdb_taxmap:
    input:  tax = ITGDB_TAX
    output:
        taxmap = os.path.join(OUT, "itgdb", "itgdb_taxmap.tsv")
    threads: Rq("itgdb_taxmap", "threads")
    resources:
        mem_mb   = Rq("itgdb_taxmap", "mem_mb"), 
        runtime  = Rq("itgdb_taxmap", "runtime"),
        partition = Rq("itgdb_taxmap", "partition"),
        account  = Rq("itgdb_taxmap", "account"),
        extra    =  R("itgdb_taxmap", "extra"),
    params:
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    container: CONTAINERS["cpu"]         
    shell: r"""
      set -euo pipefail
      mkdir -p "$(dirname "{output.taxmap}")"
      awk -F'\t' '
        BEGIN{{ OFS="\t"; print "id","taxonomy" }}
        NR==1 && ($1=="id" || $1 ~ /^#/) {{ next }}
        $1 ~ /^#/      {{ next }}
        NF >= 2        {{ print $1, $2 }}
      ' "{input.tax}" > "{output.taxmap}"
      """
    
rule itgdb_map_reads:
    input:
        reads = os.path.join(TMP, "filtered"),
        idx   = rules.itgdb_index.output.mmi
    output:
        done  = touch(os.path.join(OUT, "itgdb", ".map_reads.done"))
    threads:   Rq("itgdb_map_reads", "threads")
    resources:
        mem_mb    = Rq("itgdb_map_reads", "mem_mb"),
        runtime   = Rq("itgdb_map_reads", "runtime"),
        partition = Rq("itgdb_map_reads", "partition"),
        account   = Rq("itgdb_map_reads", "account"),
        extra     =  R("itgdb_map_reads", "extra"),
    container: CONTAINERS["nanoalign"]
    params:
        preset  = config.get("itgdb_minimap", {}).get("preset", "map-ont"),
        prim    = "--secondary=no -N 1" if config.get("itgdb_minimap", {}).get("primary_only", True) else "",
        container_rev = lambda wc: config["container_rev"].get("nanoalign","0")
    shell: r"""
      set -euo pipefail
      paf_dir="$(dirname "{output.done}")"
      mkdir -p "$paf_dir"
      shopt -s nullglob

      for fq in "{input.reads}"/*.fastq "{input.reads}"/*.fastq.gz; do
        [ -e "$fq" ] || continue
        bn=$(basename "$fq")
        sid="${{bn%.fastq.gz}}"; sid="${{sid%.fastq}}"
        out="$paf_dir/${{sid}}.paf"
        
        if [[ -s "$out" ]]; then
          echo "skip $sid (have $out)" >&2
          continue
        fi
        echo "minimap2 → $sid" >&2
        
        if ! [ -s "$fq" ] || ! zcat -f "$fq" 2>/dev/null | head -n 1 | grep -q '^@'; then
          echo "[map_reads] $bn is empty; creating empty $out" >&2
          : > "$out"
          continue
        fi
        minimap2 -t {threads} -x "{params.preset}" {params.prim} \
                 "{input.idx}" "$fq" > "$out"
      done


      touch "{output.done}"
    """
    
rule itgdb_species_tables:
    input:
        paf_done = rules.itgdb_map_reads.output.done,
        taxmap   = rules.itgdb_taxmap.output.taxmap
    output:
        merged   = os.path.join(OUT, "itgdb/species", "itgdb_species_merged.tsv")
    threads: Rq("itgdb_species_tables", "threads")
    resources:
        mem_mb   = Rq("itgdb_species_tables", "mem_mb"),
        runtime  = Rq("itgdb_species_tables", "runtime"),
        partition = Rq("itgdb_species_tables", "partition"),
        account  = Rq("itgdb_species_tables", "account"),
        extra  = R("itgdb_species_tables", "extra"),
    container: CONTAINERS["cpu"]
    params:
        mapq_min   = int(config["itgdb_minimap"].get("mapq_min", 10)),
        aln_min_bp = int(config["itgdb_minimap"].get("aln_min_bp", 1000)),
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    shell: r"""
      set -euo pipefail
      mkdir -p "$(dirname "{output.merged}")"

      PYBIN=$(command -v python || command -v python3 || true)
      [ -n "$PYBIN" ] || {{ echo "No python in container." >&2; exit 127; }}

      "$PYBIN" - <<'PY'
import os, glob, csv, sys, re
from collections import Counter

paf_dir       = os.path.dirname(r"{input.paf_done}")
taxmap_fn     = r"{input.taxmap}"
species_merge = r"{output.merged}"

species_dir = os.path.dirname(species_merge)
genus_dir   = os.path.join(os.path.dirname(species_dir), "genus")
os.makedirs(species_dir, exist_ok=True)
os.makedirs(genus_dir,   exist_ok=True)

mapq_min   = int("{params.mapq_min}")
aln_min_bp = int("{params.aln_min_bp}")

id2tax = dict()
with open(taxmap_fn, "r", encoding="utf-8", errors="ignore") as fh:
    header_seen = False
    for line in fh:
        line = line.rstrip("\n")
        if not line:
            continue
        cols = line.split("\t")
        if not header_seen:
            header_seen = True
            if cols[0].lower() in ("id", "#id", "#"):
                continue
        if len(cols) < 2:
            continue
        sid = cols[0]
        tax = cols[1]
        id2tax[sid] = tax

def _norm_target_key(tname):
    if tname in id2tax:
        return tname
    base = re.split(r"[|\s]", tname)[0]
    return base

def species_from_tax(tax):
    if not tax or tax == "NA":
        return "Unassigned"
    m = re.search(r"s__([^;|,\s]+)", tax)
    if m:
        return m.group(1).replace("_", " ").strip()
    m = re.search(r"g__([^;|,\s]+)", tax)
    if m:
        return m.group(1).replace("_", " ").strip() + " sp."
    parts = [p.strip() for p in re.split(r"[;|,]", tax) if p.strip()]
    if parts:
        guess = re.sub(r'^.*__', '', parts[-1]).replace("_", " ").strip()
        toks = guess.split()
        if len(toks) >= 2:
            return toks[-2] + " " + toks[-1]
        return guess or "Unassigned"
    return "Unassigned"

def genus_from_tax(tax):
    if not tax or tax == "NA":
        return "Unassigned"
    m = re.search(r"g__([^;|,\s]+)", tax)
    if m:
        return m.group(1).replace("_", " ").strip()
    parts = [p.strip() for p in re.split(r"[;|,]", tax) if p.strip()]
    for i, p in enumerate(parts):
        if p.startswith("s__") and i > 0:
            return re.sub(r'^.*__', '', parts[i-1]).replace("_", " ").strip()
    if parts:
        guess = re.sub(r'^.*__', '', parts[-1]).replace("_", " ").strip()
        toks = guess.split()
        if len(toks) >= 2:
            return toks[0]
        return guess or "Unassigned"
    return "Unassigned"

pafs = sorted(glob.glob(os.path.join(paf_dir, "*.paf")))
if not pafs:
    with open(species_merge, "w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, delimiter="\t").writerow(["Species"])
    with open(os.path.join(genus_dir, "itgdb_genus_merged.tsv"), "w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, delimiter="\t").writerow(["Genus"])
    sys.exit(0)

species_counts = dict()
genus_counts   = dict()
samples_seen   = []

for paf in pafs:
    sid = os.path.basename(paf)[:-4]
    samples_seen.append(sid)
    scnt = Counter()
    gcnt = Counter()

    with open(paf, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            if not line or line.startswith("#"):
                continue
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 12:
                continue
            tname = cols[5]
            try:
                aln  = int(cols[10])
                mapq = int(cols[11])
            except Exception:
                continue
            if mapq < mapq_min or aln < aln_min_bp:
                continue

            key = _norm_target_key(tname)
            tax = id2tax.get(key, "NA")

            sp = species_from_tax(tax)
            ge = genus_from_tax(tax)

            scnt[sp] += 1
            gcnt[ge] += 1

    sp_path = os.path.join(species_dir, "itgdb_species_" + sid + ".tsv")
    ge_path = os.path.join(genus_dir,   "itgdb_genus_"   + sid + ".tsv")

    with open(sp_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["Species", sid])
        for sp, c in scnt.most_common():
            w.writerow([sp, c])

    with open(ge_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["Genus", sid])
        for ge, c in gcnt.most_common():
            w.writerow([ge, c])

    species_counts[sid] = scnt
    genus_counts[sid]   = gcnt

samples = sorted(samples_seen)

all_species = sorted(set().union(*(set(sc.keys()) for sc in species_counts.values()))) if species_counts else []
with open(species_merge, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh, delimiter="\t")
    w.writerow(["Species"] + samples)
    for sp in all_species:
        w.writerow([sp] + [species_counts[s].get(sp, 0) for s in samples])

genus_merge = os.path.join(genus_dir, "itgdb_genus_merged.tsv")
all_genera  = sorted(set().union(*(set(gc.keys()) for gc in genus_counts.values()))) if genus_counts else []
with open(genus_merge, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh, delimiter="\t")
    w.writerow(["Genus"] + samples)
    for ge in all_genera:
        w.writerow([ge] + [genus_counts[s].get(ge, 0) for s in samples])
PY
    """
    
rule isonclust3:
    input: rules.fastcat_filter.output.fastq
    output: directory(os.path.join(TMP, "OTUs"))
    threads: Rq("isonclust3", "threads")
    resources:
        mem_mb   = Rq("isonclust3", "mem_mb"), 
        runtime  = Rq("isonclust3", "runtime"),
        partition = Rq("isonclust3", "partition"),
        account  = Rq("isonclust3", "account"),
        extra    =  R("isonclust3", "extra"),
    log: os.path.join(OUT, "logs/isonclust3.log")
    params:
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p {output}
      shopt -s nullglob
      for fq in {input}/*.fastq; do
        samp=$(basename "$fq" .fastq)
        outdir="{output}/${{samp}}"
        mkdir -p "$outdir"
        isONclust3 --fastq "$fq" --outfolder "$outdir" --mode ont --post-cluster
      done
    """

rule spoa_consensus:
    input: rules.isonclust3.output
    output:
        done = touch(os.path.join(TMP, "consensus_drafts/.done"))
    threads: Rq("spoa_consensus", "threads")
    resources:
        mem_mb   = Rq("spoa_consensus", "mem_mb"), 
        runtime  = Rq("spoa_consensus", "runtime"),
        partition = Rq("spoa_consensus", "partition"),
        account  = Rq("spoa_consensus", "account"),
        extra    =  R("spoa_consensus", "extra") 
    params:
        outdir     = os.path.join(TMP, "consensus_drafts"),
        max_reads  = int(config.get("spoa_max_reads", 500)),
        min_reads  = int(config.get("spoa_min_reads", 3)),
        extra      = lambda wc: config.get("spoa_extra",""),
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    log: os.path.join(OUT, "logs/spoa_consensus.log")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p "{params.outdir}" "$(dirname "{log}")"
      exec > "{log}" 2>&1
      
      shopt -s nullglob
      mapfile -t fq_list < <(find "{input}" -type f -path "*/clustering/fastq_files/*.fastq" | sort)
      
      for fq in "${{fq_list[@]}}"; do
        sample=$(basename "$(dirname "$(dirname "$fq")")")
        cid=$(basename "$fq" .fastq)
        out="{params.outdir}/${{sample}}_${{cid}}.fasta"

        if [[ -s "$out" ]]; then
          echo "skip $out" >&2
          continue
        fi

        n=$(awk 'END{{print int(NR/4)}}' "$fq")
        if (( n < {params.min_reads} )); then
          echo "skip (too few reads: $n) → $out" >&2
          continue
        fi

        tmpd=$(mktemp -d)
        tmpf="$tmpd/reads.fastq"

        if (( n > {params.max_reads} )); then
          awk -v m={params.max_reads} 'NR%4==1{{c++}} c<=m{{print}}' "$fq" > "$tmpf"
        else
          cp "$fq" "$tmpf"
        fi

        if spoa {params.extra} "$tmpf" > "$out.tmp"; then
          mv -f "$out.tmp" "$out"
        else
          echo "spoa failed on $fq" >&2
          rm -f "$out.tmp"
        fi
        rm -rf "$tmpd"
      done

      : > "{output.done}"
    """
    
rule vsearch_pool_cluster: 
    input: 
      done = rules.spoa_consensus.output.done
    output:
        drafts = os.path.join(TMP, "pooled/all_draft_otus.fasta"),
        cent99 = os.path.join(OUT, "otu/otus_centroids_99.fasta"),
        cent97 = os.path.join(OUT, "otu/otus_centroids_97.fasta")
    threads: Rq("vsearch_pool_cluster", "threads")
    resources:
        mem_mb   = Rq("vsearch_pool_cluster", "mem_mb"), 
        runtime  = Rq("vsearch_pool_cluster", "runtime"),
        partition = Rq("vsearch_pool_cluster", "partition"),
        account  = Rq("vsearch_pool_cluster", "account"),
        extra    =  R("vsearch_pool_cluster", "extra") 
    params:
        consdir    = os.path.join(TMP, "consensus_drafts"),
        pooldir    = os.path.join(TMP, "pooled"),
        container_rev = lambda wc: config["container_rev"].get("cpu","0"),
        id_primary = lambda wc: float(config["otu_id_primary"]),
        id_legacy  = lambda wc: float(config["otu_id_legacy"]),
        min_unique = lambda wc: int(config["min_unique_size"])
    log: os.path.join(OUT, "logs/vsearch_pool_cluster.log")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p "{params.pooldir}" "$(dirname "{output.cent99}")" "$(dirname "{log}")"
      exec > "{log}" 2>&1
      
      shopt -s nullglob
      files=( "{params.consdir}"/*.fasta "{params.consdir}"/*.fa "{params.consdir}"/*.fna )
      
      if (( ${{#files[@]}} == 0 )); then
        echo "[vsearch_pool_cluster] No consensus FASTA files in {params.consdir}" >&2
        : > "{output.drafts}"; : > "{output.cent99}"; : > "{output.cent97}"
        exit 0
      fi
      
      cat "${{files[@]}}" > "{output.drafts}"
      
      if ! grep -q '^>' "{output.drafts}"; then
        echo "[vsearch_pool_cluster] Drafts contain 0 sequences." >&2
        : > "{output.cent99}"; : > "{output.cent97}"
        exit 0
      fi
      
      command -v vsearch >/dev/null || {{ echo "vsearch not found"; exit 127; }}
      
      vsearch --derep_fulllength "{output.drafts}" \
              --sizeout --relabel OTU_ --strand both \
              --minuniquesize {params.min_unique} --threads {threads} \
              --output "{params.pooldir}/otus_derep.fasta"
      
      if ! grep -q '^>' "{params.pooldir}/otus_derep.fasta"; then
        echo "[vsearch_pool_cluster] Derep produced 0 sequences (minuniquesize={params.min_unique}). Passing drafts through as centroids." >&2
        cp "{output.drafts}" "{output.cent99}"
        cp "{output.drafts}" "{output.cent97}"
      else
        vsearch --cluster_fast "{params.pooldir}/otus_derep.fasta" \
                --id {params.id_primary} --strand both \
                --centroids "{output.cent99}" --threads {threads}
      
        vsearch --cluster_fast "{params.pooldir}/otus_derep.fasta" \
                --id {params.id_legacy}  --strand both \
                --centroids "{output.cent97}" --threads {threads}
      fi
      
      for f in "{output.drafts}" "{params.pooldir}/otus_derep.fasta" "{output.cent99}" "{output.cent97}"; do
        printf "[counts] %-40s %6d\n" "$f" "$(grep -c '^>' "$f" || true)"
      done
    """

rule uniqify_otu_centroids:
    input:
        cent99 = rules.vsearch_pool_cluster.output.cent99  ,
        cent97 = rules.vsearch_pool_cluster.output.cent97  
    output:
        cent99_uniq = os.path.join(OUT, "otu/otus_centroids_99.uniq.fasta"),
        cent97_uniq = os.path.join(OUT, "otu/otus_centroids_97.uniq.fasta")
    threads: Rq("uniqify_otu_centroids", "threads")
    resources:
        mem_mb   = Rq("uniqify_otu_centroids", "mem_mb"), 
        runtime  = Rq("uniqify_otu_centroids", "runtime"),
        partition = Rq("uniqify_otu_centroids", "partition"),
        account  = Rq("uniqify_otu_centroids", "account"),
        extra    =  R("uniqify_otu_centroids", "extra") 
    log: os.path.join(OUT, "logs/uniqify_otu_centroids.log")
    container: CONTAINERS["cpu"]
    shell: r"""
        set -euo pipefail
        mkdir -p "$(dirname {output.cent99_uniq})" "$(dirname {output.cent97_uniq})"

        vsearch --fastx_filter {input.cent99} --relabel OTU_ --fasta_width 0 --fastaout {output.cent99_uniq}
        vsearch --fastx_filter {input.cent97} --relabel OTU_ --fasta_width 0 --fastaout {output.cent97_uniq}

        dup=$(grep '^>' {output.cent99_uniq} | sed 's/^>//' | sort | uniq -d | head -n1 || true)
        if [ -n "${{dup:-}}" ]; then
          echo "[uniqify_otu_centroids] Duplicate header in {output.cent99_uniq}: $dup" >&2
          exit 1
        fi
    """

rule map_all_reads:
    input:
        reads = rules.fastcat_filter.output.fastq,        
        refs  = rules.uniqify_otu_centroids.output.cent99_uniq 
    output:
        all_reads = ALL_READS_FQ,   
        bam       = MAP_BAM_R0      
    threads: Rq("map_all_reads", "threads")
    resources:
        mem_mb   = Rq("map_all_reads", "mem_mb"),
        runtime  = Rq("map_all_reads", "runtime"),
        partition = Rq("map_all_reads", "partition"),
        account  = Rq("map_all_reads", "account"),
        extra    = R("map_all_reads", "extra")
    log: os.path.join(OUT, "logs/map_all_reads.log")
    params:
        container_rev = lambda wc: config["container_rev"].get("nanoalign","0")
    container: CONTAINERS["nanoalign"]
    shell: r"""
      set -euo pipefail
      mkdir -p "$(dirname "{output.all_reads}")" "$(dirname "{output.bam}")"

      : > "{output.all_reads}"
      find "{input.reads}" -maxdepth 1 -type f \
        \( -name '*.fastq' -o -name '*.fastq.gz' \) -print0 \
      | xargs -0 -r -I% bash -c '
        fq="$1"
        if [[ "$fq" == *.gz ]]; then gzip -dc "$fq"; else cat "$fq"; fi
      ' _ % >> "{output.all_reads}"
      
      if ! grep -q "^@" "{output.all_reads}"; then
        echo "[map_all_reads] No reads after filtering; creating header-only BAM." >&2
        samtools faidx "{input.refs}"
        awk 'BEGIN{{OFS="\t"}}{{print "@SQ\tSN:"$1"\tLN:"$2}}' "{input.refs}.fai" > "{output.bam}.header.sam"
        samtools view -b -o "{output.bam}" "{output.bam}.header.sam"
        rm -f "{output.bam}.header.sam"
        samtools index -@ {threads} "{output.bam}"
        exit 0
      fi
      
      minimap2 -t {threads} -ax map-ont "{input.refs}" "{output.all_reads}" \
        | samtools sort -@ {threads} -m 2G -o "{output.bam}"
      samtools index -@ {threads} "{output.bam}"
    """

# racon_round1
rule racon_round1:
    input: 
      reads = rules.map_all_reads.output.all_reads, 
      bam   = rules.map_all_reads.output.bam, 
      refs  = rules.uniqify_otu_centroids.output.cent99_uniq 
    output: 
      r1 = R1_FASTA
    threads: Rq("racon_round1", "threads")
    resources:
        mem_mb   = Rq("racon_round1", "mem_mb"), 
        runtime  = Rq("racon_round1", "runtime"),
        partition = Rq("racon_round1", "partition"),
        account  = Rq("racon_round1", "account"),
        extra    =  R("racon_round1", "extra") 
    params:
        container_rev = lambda wc: config["container_rev"].get("nanoalign","0")
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

        refs=$(readlink -f "{input.refs}")
        if ! grep -q '^>' "$refs"; then
          echo "[racon_round1] Reference FASTA has no sequences; emitting empty polished file." >&2
          : > "{output.r1}"
          exit 0
        fi
        
        if ! [ -s "{input.reads}" ] || ! grep -q '^@' "{input.reads}"; then
          echo "[racon_round1] No reads; copying references to polished output." >&2
          cp "$refs" "{output.r1}"
          exit 0
        fi
        
        if ! samtools quickcheck -v "{input.bam}"; then
          echo "[racon_round1] BAM failed quickcheck; copying references." >&2
          cp "$refs" "{output.r1}"
          exit 0
        fi
        if [ "$(samtools view -c "{input.bam}")" -eq 0 ]; then
          echo "[racon_round1] BAM has zero alignments; copying references." >&2
          cp "$refs" "{output.r1}"
          exit 0
        fi
        
        tmp_paf=$(mktemp --suffix=.paf)
        trap 'rm -f "$tmp_paf"' EXIT
        minimap2 -t {threads} -x map-ont "$refs" "{input.reads}" > "$tmp_paf"
        
        if ! [ -s "$tmp_paf" ]; then
          echo "[racon_round1] No overlaps in PAF; copying references." >&2
          cp "$refs" "{output.r1}"
          exit 0
        fi
        
        racon -t {threads} "{input.reads}" "$tmp_paf" "$refs" > "{output.r1}"
        test -s "{output.r1}" && grep -q "^>" "{output.r1}"
    """
    
rule map_r1:
    input: 
      reads = rules.map_all_reads.output.all_reads,   
      r1    = rules.racon_round1.output.r1            
    output: 
      bam = MAP_BAM_R1
    threads: Rq("map_r1", "threads")
    resources:
        mem_mb   = Rq("map_r1", "mem_mb"), 
        runtime  = Rq("map_r1", "runtime"),
        partition = Rq("map_r1", "partition"),
        account  = Rq("map_r1", "account"),
        extra    = R("map_r1", "extra") 
    params:
        container_rev = lambda wc: config["container_rev"].get("nanoalign","0")
    container: CONTAINERS["nanoalign"]
    shell: r"""
        set -euo pipefail
        mkdir -p "$(dirname "{output.bam}")"

        if ! grep -q "^@" "{input.reads}"; then
        echo "[map_r1] No reads; creating header-only BAM." >&2
          samtools faidx "{input.r1}"
          awk 'BEGIN{{OFS="\t"}}{{print "@SQ\tSN:"$1"\tLN:"$2}}' "{input.r1}.fai" > "{output.bam}.header.sam"
          samtools view -b -o "{output.bam}" "{output.bam}.header.sam"
          rm -f "{output.bam}.header.sam"
          samtools index -@ {threads} "{output.bam}"
          exit 0
        fi

        minimap2 -t {threads} -ax map-ont "{input.r1}" "{input.reads}" \
          | samtools sort -@ {threads} -m 2G -o "{output.bam}"
        samtools index -@ {threads} "{output.bam}"
    """    
# racon_round2
rule racon_round2:
    input: 
      reads = rules.map_all_reads.output.all_reads, 
      bam   = rules.map_r1.output.bam, 
      r1    = rules.racon_round1.output.r1
    output: 
      r2 = R2_FASTA
    threads: Rq("racon_round2", "threads")
    resources:
        mem_mb   = Rq("racon_round2", "mem_mb"), 
        runtime  = Rq("racon_round2", "runtime"),
        partition = Rq("racon_round2", "partition"),
        account  = Rq("racon_round2", "account"),
        extra    =  R("racon_round2", "extra") 
    params:
        container_rev = lambda wc: config["container_rev"].get("nanoalign","0")
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
        reads = rules.map_all_reads.output.all_reads,
        draft = rules.racon_round2.output.r2
    output:
        polished = POLISHED
    threads: Rq("medaka_polish", "threads")
    resources:
        mem_mb         = Rq("medaka_polish", "mem_mb"),
        runtime        = Rq("medaka_polish", "runtime"),
        slurm_partition = Rq("medaka_polish", "slurm_partition"),
        slurm_account  = Rq("medaka_polish", "slurm_account"),
        slurm_extra    =  R("medaka_polish", "slurm_extra", "")    
    params:
        container_rev = lambda wc: config["container_rev"].get("gpu","0"),
        medaka_model  = lambda wc: config["medaka_model"],
        polish_dir    = POLISH_DIR  
    container:
        CONTAINERS["gpu"]
    shell: r"""
        set -euo pipefail
        export OMP_NUM_THREADS=1
        mkdir -p "{params.polish_dir}/medaka_refined"

        medaka_consensus \
          -i "{input.reads}" \
          -d "{input.draft}" \
          -o "{params.polish_dir}/medaka_refined" \
          -m "{params.medaka_model}" \
          -t {threads} \
          --bacteria

        cp "{params.polish_dir}/medaka_refined/consensus.fasta" "{output.polished}"
        """

##############################################
#  POST-POLISH OTU PROCESSING STAGE
#  (Runs after medaka_polish)
##############################################

# 1. Identify chimeras + assign taxonomy
rule chimera_taxonomy:
    input:
        fasta = rules.medaka_polish.output.polished,
        db    = ITGDB_UDB
    output:
        nonchim = os.path.join(OUT, "otu/otus_clean.fasta"),
        chimera = os.path.join(OUT, "otu/otus_chimeras.fasta"),
        sintax  = os.path.join(OUT, "otu/otus_taxonomy.sintax")
    resources:
        mem_mb    = Rq("chimera_taxonomy", "mem_mb"),
        runtime   = Rq("chimera_taxonomy", "runtime"),
        partition = Rq("chimera_taxonomy", "partition"),
        account   = Rq("chimera_taxonomy", "account"),
        extra     = R("chimera_taxonomy", "extra"),
    params:
        ref_nonchim = os.path.join(OUT, "otu/otus_clean.refok.fasta"),
        ref_chimera = os.path.join(OUT, "otu/otus_chimeras.ref.fasta"),
        cutoff      = lambda wc: SINTAX_CUTOFF,
        container_rev = lambda wc: config["container_rev"].get("cpu","0"),
        out_dir       = OUT,
    threads: Rq("chimera_taxonomy", "threads")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      command -v vsearch >/dev/null || {{ echo "vsearch not found"; exit 127; }}
      mkdir -p "{params.out_dir}/otu"
      
      if ! grep -q '^>' "{input.fasta}"; then
        echo "[chimera_taxonomy] Input FASTA empty; creating empty outputs." >&2
        : > "{output.nonchim}"
        : > "{output.chimera}"
        : > "{output.sintax}"
        exit 0
      fi

      vsearch --uchime_denovo "{input.fasta}" \
              --nonchimeras "{output.nonchim}" \
              --chimeras    "{output.chimera}" \
              --threads {threads}


      # If denovo produced nothing, emit empty taxonomy and exit cleanly
      if ! grep -q '^>' "{output.nonchim}"; then
        : > "{output.sintax}"
        exit 0
      fi

      vsearch --uchime_ref "{output.nonchim}" \
              --db "{input.db}" \
              --nonchimeras "{params.ref_nonchim}" \
              --chimeras    "{params.ref_chimera}" \
              --threads {threads}
      mv -f "{params.ref_nonchim}" "{output.nonchim}"
      
      # If ref-based step removed everything, emit empty taxonomy and exit
      if ! grep -q '^>' "{output.nonchim}"; then
        : > "{output.sintax}"
        exit 0
      fi

      vsearch --sintax "{output.nonchim}" \
              --db "{input.db}" \
              --sintax_cutoff {params.cutoff} \
              --strand both \
              --tabbedout "{output.sintax}" \
              --threads {threads}
    """
    
rule collapse_ultraclose:
    input:
        fasta = rules.chimera_taxonomy.output.nonchim
    output:
        fasta = os.path.join(OUT, "otu/otus_ultraclose_merged.fasta")
    params:
        derep         = os.path.join(OUT, "otu/otus_ultraclose_merged.derep.fasta"),
        id            = lambda wc: COLLAPSE_ID,
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    resources:
        mem_mb    = Rq("collapse_ultraclose", "mem_mb"),
        runtime   = Rq("collapse_ultraclose", "runtime"),
        partition = Rq("collapse_ultraclose", "partition"),
        account   = Rq("collapse_ultraclose", "account"),
        extra     = R("collapse_ultraclose", "extra"),
    threads: Rq("collapse_ultraclose", "threads")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      vsearch --derep_fulllength "{input.fasta}" \
              --sizein --sizeout --relabel OTU_ \
              --output "{params.derep}" --threads {threads}

      vsearch --cluster_fast "{params.derep}" \
              --id {params.id} --strand both \
              --centroids "{output.fasta}" --threads {threads}
    """

rule otu_alignment:
    input:
        fasta = rules.collapse_ultraclose.output.fasta
    output:
        msa = os.path.join(OUT, "otu/otu_references_aligned.fasta")
    threads: Rq("otu_alignment", "threads")
    resources:
        mem_mb    = Rq("otu_alignment", "mem_mb"),
        runtime   = Rq("otu_alignment", "runtime"),
        partition = Rq("otu_alignment", "partition"),
        account   = Rq("otu_alignment", "account"),
        extra     = R("otu_alignment", "extra")
    params:
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    container: CONTAINERS["cpu"]
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
    params:
        container_rev = lambda wc: config["container_rev"].get("cpu","0"),
        out_dir       = OUT
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
    
      mkdir -p "{params.out_dir}/otu" "$(dirname "{log}")"
    
      IQ=$(command -v iqtree || command -v iqtree3 || command -v iqtree2 || true)
      if [ -z "$IQ" ]; then
        echo "No iqtree executable found in container PATH." >&2
        exit 127
      fi
    
      ckp="{params.out_dir}/otu/otu_tree.ckp.gz"
      if [ -f "$ckp" ]; then
        echo "[iqtree3_tree] removing stale checkpoint $ckp" >> "{log}" 2>&1 || true
        rm -f "$ckp" || true
      fi
    
      "$IQ" -s "{input.msa}" -nt {threads} -m TEST -bb 1000 -alrt 1000 \
            -pre "{params.out_dir}/otu/otu_tree" >> "{log}" 2>&1 || true
    
      if [ ! -s "{output.tree}" ]; then
        "$IQ" -s "{input.msa}" -nt {threads} -m TEST -bb 1000 -alrt 1000 \
              -pre "{params.out_dir}/otu/otu_tree" -redo >> "{log}" 2>&1
      fi
    
      test -s "{output.tree}"
    """
    
# 4. Per-sample read counts for each OTU
rule otu_table_per_sample:
    input:
        refs  = rules.collapse_ultraclose.output.fasta,
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
        container_rev = lambda wc: config["container_rev"].get("cpu","0"),
        map_id        = lambda wc: MAP_ID,
        strand        = lambda wc: STRAND,
        iddef         = 1,
        qcov          = 0.90,
        tcov          = 0.90,
        qmask         = "none",
        tables_dir    = os.path.join(OUT, "otu", "tables")
    container: CONTAINERS["cpu"]
    shell:
      r"""
      set -euo pipefail
      mkdir -p "{params.tables_dir}"
      shopt -s nullglob

      PYBIN=$(command -v python || command -v python3 || true)
      [ -n "$PYBIN" ] || {{ echo "No python interpreter found."; exit 127; }}

      for fq in "{input.reads}"/*.fastq; do
        sid=$(basename "$fq" .fastq)
        sid=$(basename "$fq" .fastq)
        vsearch --usearch_global "$fq" \
                --db "{input.refs}" \
                --id {params.map_id} \
                --iddef {params.iddef} \
                --query_cov {params.qcov} \
                --target_cov {params.tcov} \
                --strand {params.strand} \
                --qmask {params.qmask} \
                --otutabout "{params.tables_dir}/otu_table_${{sid}}.tsv" \
                --threads {threads}
      done

"$PYBIN" - <<'PY'
import glob, os, csv, sys

def to_int(x):
    try:
        s = str(x).strip()
        if not s or s.upper() == "NA":
            return 0
        return int(float(s))
    except Exception:
        return 0

out = r"{output.merged}"
tables = sorted(glob.glob(os.path.join(r"{params.tables_dir}", "otu_table_*.tsv")))
if not tables:
    with open(out, "w", newline="") as fh:
        csv.writer(fh, delimiter="\t").writerow(["OTU"])
    sys.exit(0)

merged = {{}}
samples = []

for t in tables:
    sid = os.path.basename(t)
    if sid.startswith("otu_table_"): sid = sid[len("otu_table_"):]
    if sid.endswith(".tsv"):         sid = sid[:-4]
    samples.append(sid)

    with open(t, newline="") as fh:
        r = csv.reader(fh, delimiter="\t")
        for row in r:
            if not row:                      
                continue
            if row[0].startswith("#"):       
                continue
            if row[0].upper() in {{"OTU","OTUID","OTU_ID","OTUId"}}:
                continue
            otu = row[0]
            val = sum(to_int(x) for x in row[1:])
            merged.setdefault(otu, {{}})
            merged[otu][sid] = merged[otu].get(sid, 0) + val

with open(out, "w", newline="") as fh:
    w = csv.writer(fh, delimiter="\t")
    w.writerow(["OTU"] + samples)
    for otu in sorted(merged):
        w.writerow([otu] + [merged[otu].get(s, 0) for s in samples])
PY
      """

