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

import re

def _cfg(path, default=None):
    cur = config
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur: return default
        cur = cur[k]
    return cur

def normalize_sample_id(s):
    """Make sample ids from routing files, BAM/FQ names, etc. comparable."""
    if s is None: return ""
    x = str(s)

    tok = _cfg("names.drop_after", "_")
    keep_right = bool(_cfg("names.keep_right", True))  # default: keep right of token
    if tok and tok in x:
        left, right = x.split(tok, 1)
        x = right if keep_right else left

    for pat, repl in _cfg("names.replace", []):
        x = re.sub(pat, repl, x)

    for suf in _cfg("names.strip_suffix", []):
        x = re.sub(f"{suf}$", "", x)

    if _cfg("names.lower", False):
        x = x.lower()

    return x.strip()
  
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
    "tree":      _expand(config.get("container_tree",      "$PROJ_ROOT/containers/nanotree.sif")),
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

MAP_ID  = float(config.get("map_id", 0.90))
COLLAPSE_ID  = float(config.get("collapse_id", 0.997))
STRAND  = config.get("strand", "both")
SINTAX_CUTOFF = float(config.get("sintax_cutoff", 0.5))
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

# Grouped, labeled path templates you can reuse in params/logs
PATH_T = {
    "run": {
        "basecall_dir": BASECALL_DIR_T,
        "demux_dir":    DEMUX_DIR_T,
        "summary_dir":  SUMMARY_DIR_T,
        "raw_dir":      RAW_DIR_T,
        "logs_basecall": LOG_BASECALL_T,
        "logs_trim":     LOG_TRIM_T,
    },
    "dset": {
        "root":        OUT,
        "tmp":         TMP,
        "qc_dir":      os.path.join(OUT, "qc"),
        "qc_hist":     os.path.join(OUT, "qc", "fastcat-histograms"),
        "otu_dir":     os.path.join(OUT, "otu"),
        "itgdb_dir":   os.path.join(OUT, "itgdb"),
        "wf16s_in":    os.path.join(TMP, "wf16s_in"),
        "filtered":    os.path.join(TMP, "filtered"),
    }
}

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

def load_routing_for_dataset(dataset):
    """
    Return a dict: normalized_sample -> desired run (or None if unspecified).
    TSV columns: sample, dataset, [run]
    """
    picks = {}
    if not ROUTING_TSV or not Path(ROUTING_TSV).exists():
        return picks
    with open(ROUTING_TSV) as fh:
        rdr = csv.DictReader(fh, delimiter="\t")
        for row in rdr:
            if row.get("dataset","").strip() == dataset:
                s_raw = row.get("sample","").strip()
                r = (row.get("run","") or "").strip() or None
                if s_raw:
                    picks[normalize_sample_id(s_raw)] = r
    return picks

# Load routing once and expose helpers
ROUTE = load_routing_for_dataset(DATASET)   # dict: normalized sample -> run or None
ROUTE_KEYS = set(ROUTE.keys())

def sample_allowed(sample, run):
    key = normalize_sample_id(sample)
    want = ROUTE.get(key, None)
    # must be routed to this dataset, and either run unspecified or matches
    return (key in ROUTE) and (want is None or want == run)
  
def enabled_unknowns():
    return bool(config.get("unknowns", {}).get("enable", False))

UNKNOWN_DIR = os.path.join(OUT, "unknowns")
Path(UNKNOWN_DIR).mkdir(parents=True, exist_ok=True)
UNKNOWN_POOL = os.path.join(UNKNOWN_DIR, "unknown_pool.fastq")
UNKNOWN_DEREP = os.path.join(UNKNOWN_DIR, "unknown_derep.fasta")
UNKNOWN_CENT  = os.path.join(UNKNOWN_DIR, "unknown_centroids.fasta")
UNKNOWN_CENT_FILT = os.path.join(UNKNOWN_DIR, "unknown_centroids.minabund.fasta")
UNKNOWN_TABLE_DIR = os.path.join(OUT, "otu", "unknown_tables")
UNKNOWN_TABLE_MERGED = os.path.join(OUT, "otu", "otu_table_unknown.tsv")

def enabled_pooled():
    return bool(config.get("pooled", {}).get("enable", False))

POOLED_FQ      = os.path.join(TMP, "pooled_reads", config.get("pooled",{}).get("concat_name","all_samples_pooled.fastq"))
POOLED_OTU_DIR = os.path.join(TMP, "OTUs_pooled")
POOLED_CONS_DIR= os.path.join(TMP, "consensus_drafts_pooled")
  
# ---- Build final targets safely (no functions leak into input) ----
_demux_done = [os.path.join(raw_dir_for_run(r), "demux_trim.done") for r in RUNS]

# move these up, above _final_targets
OTU_TAX_BEST   = os.path.join(OUT, "otu", "otu_taxonomy_best.tsv")
OTU_TABLE_KNOWN = os.path.join(OUT, "otu", "otu_table_merged.tsv")
OTU_TABLE_ALL   = os.path.join(OUT, "otu", "otu_table_with_unknowns.tsv")
OTU_TABLE_FINAL = os.path.join(OUT, "otu", "otu_table_final.tsv")

_final_targets = (
    _demux_done
    + [
        os.path.join(TMP, "preflight.ok"),
        os.path.join(TMP, "dorado_all_runs.ok"),
        os.path.join(OUT, "manifest.txt"),
        os.path.join(OUT, "benchmarks/fastcat_filter.tsv"),
        os.path.join(OUT, "qc", "nanoplot", ".done"),
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

_final_targets = tuple(_final_targets) + (
    OTU_TAX_BEST,
    OTU_TABLE_ALL,
    OTU_TABLE_FINAL,
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

        must_have = ("cpu","gpu","nanoalign","dorado", "tree")
        for k in must_have:
            img = CONTAINERS.get(k, "")
            if not img:
                raise WorkflowError(f"Container path/URI for '{k}' not set.")
              
rule manifest:
    output: os.path.join(OUT, "manifest.txt")
    container: CONTAINERS["cpu"]
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
    keep = []
    with open(idx) as fh:
        rdr = _csv.DictReader(fh, delimiter="\t")
        for row in rdr:
            if row["run"] != wc.run:
                continue
            s_demux = row["sample"]
            if normalize_sample_id(s_demux) in ROUTE_KEYS:
                keep.append(s_demux)   
    if not keep:
        return []
    return sorted(set(keep))

def bam_for_sample_run(wc):
    import csv as _csv
    ck = checkpoints.demux_index_one_run.get(run=wc.run)
    idx = ck.output.tsv
    want = normalize_sample_id(wc.sample)
    cands = []
    with open(idx) as fh:
        rdr = _csv.DictReader(fh, delimiter="\t")
        for row in rdr:
            if row["run"] == wc.run and normalize_sample_id(row["sample"]) == want:
                cands.append(row["bam"])
    if not cands:
        raise WorkflowError(f"No BAM for run={wc.run} sample={wc.sample} (normalized={want}) in {idx}")
    return sorted(cands)[-1]

def trim_fastqs_for_run(wc):
    return expand(os.path.join(raw_dir_for_run(wc.run), "{sample}.fastq"),
                  sample=_samples_for_run_after_demux(wc))

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
        outdir_base = PATH_T["dset"]["root"],
        histdir     = PATH_T["dset"]["qc_hist"],
        container_rev = lambda wc: config["container_rev"].get("cpu","0"),
        min_q   = lambda wc: config["min_qscore"],
        minlen  = lambda wc: config["minlength"],
        maxlen  = lambda wc: config["maxlength"],
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
        base=$(basename "$fq")

        for pat in {params.exclude}; do
          [[ "$pat" == "__NONE__" ]] && break
          case "$base" in $pat) continue 2;; 
          esac
        done
        
        stem=$(printf "%s" "$base" | sed -E 's/\.fastq(\.gz)?$//; s/\.fq(\.gz)?$//')
      
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
        outdir = directory(os.path.join(OUT, "qc/nanoplot")),
        done   = touch(os.path.join(OUT, "qc/nanoplot", ".done"))
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
      mkdir -p {output.outdir}
      cat {input.filt_dir}/*.fastq > {output.outdir}/all.fastq
      NanoPlot --fastq {output.outdir}/all.fastq -o {output.outdir} --drop_outliers \
        --maxlength {params.maxlen} --minlength {params.minlen}
      : > {output.done}
    """

# ---------------- wf-16s branch --------------

rule stage_wf16s_input:
    input:
        filt_done = rules.fastcat_filter.output.done,
        filt_dir  = rules.fastcat_filter.output.fastq
    output:
        indir = directory(PATH_T["dset"]["wf16s_in"]),
        done  = touch(os.path.join(PATH_T["dset"]["wf16s_in"], ".staged.ok"))
    threads: Rq("stage_wf16s_input", "threads")
    resources:
        mem_mb   = Rq("stage_wf16s_input", "mem_mb"), 
        runtime  = Rq("stage_wf16s_input", "runtime"),
        partition = Rq("stage_wf16s_input", "partition"),
        account  = Rq("stage_wf16s_input", "account"),
        extra    =  R("stage_wf16s_input", "extra") 
    params:
        drop_after = lambda wc: (config.get("names", {}) or {}).get("drop_after", "__"),
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    container: CONTAINERS["cpu"]
    run:
        import os, glob, shutil, sys
        in_dir  = input.filt_dir
        out_dir = output.indir
        token   = params.drop_after

        os.makedirs(out_dir, exist_ok=True)

        # Gather both .fastq and .fastq.gz produced by fastcat_filter
        files = sorted(glob.glob(os.path.join(in_dir, "*.fastq"))) + \
                sorted(glob.glob(os.path.join(in_dir, "*.fastq.gz")))

        kept, skipped = 0, 0
        for fq in files:
            base = os.path.basename(fq)
            stem = base
            if stem.endswith(".fastq.gz"): stem = stem[:-9]
            elif stem.endswith(".fastq"):  stem = stem[:-6]

            # fastcat_filter names are "run__sample" (or whatever token you set)
            run_id, sample_part = None, stem
            if token and token in stem:
                run_id, sample_part = stem.split(token, 1)
            else:
                # fallback: last "_" chunk is sample, rest is run
                parts = stem.split("_")
                if len(parts) >= 2:
                    run_id, sample_part = "_".join(parts[:-1]), parts[-1]

            # Gate by routing: only stage samples that belong to this DATASET (and run, if constrained)
            if not sample_allowed(sample_part, run_id):
                skipped += 1
                continue

            norm_sid = normalize_sample_id(sample_part)
            sdir = os.path.join(out_dir, norm_sid)
            os.makedirs(sdir, exist_ok=True)
            link_path = os.path.join(sdir, base)
            def _same_target(a, b):
                try:
                    return os.path.abspath(a) == os.path.abspath(b)
                except Exception:
                    return False
            
            if os.path.islink(link_path):
                cur = os.readlink(link_path)
                if _same_target(cur, fq):
                    kept += 1
                    continue
            
            try:
                if os.path.exists(link_path) or os.path.islink(link_path):
                    os.remove(link_path)
                os.symlink(os.path.abspath(fq), link_path)
            except OSError:
                try:
                    if os.path.exists(link_path):
                        os.remove(link_path)
                    os.link(fq, link_path)
                except OSError:
                    shutil.copy2(fq, link_path)
            
            kept += 1

        sys.stderr.write(f"[stage_wf16s_input] staged {kept} files; skipped {skipped} (not routed)\n")
    
rule wf16s_run:
    input:
        staged = rules.stage_wf16s_input.output.done,
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
        indir   = directory(PATH_T["dset"]["wf16s_in"]),
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
    
      nf_cmd=( nextflow run "{params.repo}"
               --fastq "{params.indir}"
               --min_len "{params.minlen}"
               --min_read_qual "{params.min_q}"
               --max_len "{params.maxlen}"
               -profile "$nf_profile"
               --out_dir "{params.outdir}"
               {params.extra}
               -w "{params.workdir}"
               -resume )
    
      if [ -n "{params.rev}" ]; then nf_cmd+=( -r "{params.rev}" ); fi

      echo "=== wf-16s via Snakemake ===" >&2
      printf "%q " "${{nf_cmd[@]}}" >&2; echo >&2

      # Run with plain streaming output
      "${{nf_cmd[@]}}"
      
      : > "{output.done}"
    """

# ---------------- OTU branch ----------------
rule itgdb_index:
    input:  fasta = ancient(ITGDB_FASTA)
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
      
def paf_targets(wc):
    import glob, os
    fq_dir = rules.fastcat_filter.output.fastq
    fqs = sorted(glob.glob(os.path.join(fq_dir, "*.fastq")) +
                 glob.glob(os.path.join(fq_dir, "*.fastq.gz")))
    stems = [os.path.basename(f).replace(".fastq.gz","").replace(".fastq","") for f in fqs]
    return expand(os.path.join(OUT, "itgdb", "{sid}.paf"), sid=stems)

rule itgdb_map_reads:
    input:
        reads = rules.fastcat_filter.output.fastq,
        filt_done = rules.fastcat_filter.output.done,
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
        tmp="${{out}}.tmp"
        err="${{out}}.stderr"
        
        if [[ -s "$out" ]]; then
          echo "skip $sid (have $out)" >&2
          continue
        fi
        
        looks_fastq=false
        case "$fq" in
          *.fastq.gz)
            if zcat -q "$fq" 2>/dev/null | head -n1 | grep -q '^@'; then
              looks_fastq=true
            fi
            ;;
          *.fastq)
            if head -n1 "$fq" | grep -q '^@'; then
              looks_fastq=true
            fi
            ;;
        esac

        if ! $looks_fastq; then
          echo "[map_reads] $bn does not look like FASTQ or is empty; creating empty $out" >&2
          : > "$out"
          continue
        fi

        echo "minimap2 → $sid" >&2
        if minimap2 -t {threads} -x "{params.preset}" {params.prim} \
            "{input.idx}" "$fq" > "$tmp" 2> "$err"; then
          mv -f "$tmp" "$out"
        else
          echo "[map_reads] minimap2 failed for $bn (see $err); leaving empty $out" >&2
          : > "$out"
          rm -f "$tmp"
        fi
      done

      touch "{output.done}"
    """

# Per-sample unknown (unmapped) read extraction
def _fq_for_stem(wc):
    import os
    plain = os.path.join(PATH_T["dset"]["filtered"], f"{wc.stem}.fastq")
    gz = plain + ".gz"
    return gz if os.path.exists(gz) else plain

def _paf_for_stem(wc):
    import os
    return os.path.join(OUT, "itgdb", f"{wc.stem}.paf")

def _filtered_stems():
    import glob, os
    d = rules.fastcat_filter.output.fastq
    fqs = sorted(glob.glob(os.path.join(d, "*.fastq")) +
                 glob.glob(os.path.join(d, "*.fastq.gz")))
    return [os.path.basename(f).replace(".fastq.gz","").replace(".fastq","") for f in fqs]

rule unknown_extract_p_samp:
    input:
        fq = _fq_for_stem,
        done  = rules.itgdb_map_reads.output.done    
    output:
        unk = os.path.join(UNKNOWN_DIR, "{stem}.unknown.fastq")
    threads:   Rq("unknown_extract_p_samp", "threads")
    resources:
        mem_mb    = Rq("unknown_extract_p_samp", "mem_mb"),
        runtime   = Rq("unknown_extract_p_samp", "runtime"),
        partition = Rq("unknown_extract_p_samp", "partition"),
        account   = Rq("unknown_extract_p_samp", "account"),
        extra     = R("unknown_extract_p_samp", "extra"),
    params:
        paf   = lambda wc: os.path.join(PATH_T["dset"]["itgdb_dir"], f"{wc.stem}.paf"),
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    container: CONTAINERS["cpu"]
    run:
        import os, gzip
        mapped = set()
        paf = params.paf
        if os.path.exists(paf) and os.path.getsize(paf) > 0:
            with open(paf, "r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    if not line or line.startswith("#"): 
                        continue
                    q = line.split("\t", 1)[0].split()[0]
                    mapped.add(q)

        def opener(path):
            return gzip.open(path, "rt") if path.endswith(".gz") else open(path, "r", encoding="utf-8")

        os.makedirs(os.path.dirname(output.unk), exist_ok=True)
        with opener(input.fq) as fin, open(output.unk, "w", encoding="utf-8") as fo:
            while True:
                h = fin.readline()
                if not h: break
                s = fin.readline(); p = fin.readline(); q = fin.readline()
                if not q: break
                sid = h[1:].strip().split()[0] if h.startswith("@") else h.strip().split()[0]
                if sid not in mapped:
                    fo.write(h); fo.write(s); fo.write(p); fo.write(q)


# 2.2 subsample each unknowns file
rule unknown_subsample:
    input:
        unk = rules.unknown_extract_p_samp.output.unk
    output:
        sub = os.path.join(UNKNOWN_DIR, "{stem}.unknown.fastq.sub.fastq")
    threads:   Rq("unknown_subsample", "threads")
    resources:
        mem_mb    = Rq("unknown_subsample", "mem_mb"),
        runtime   = Rq("unknown_subsample", "runtime"),
        partition = Rq("unknown_subsample", "partition"),
        account   = Rq("unknown_subsample", "account"),
        extra     = R("unknown_subsample", "extra"),
    params:
        container_rev = lambda wc: config["container_rev"].get("cpu","0"),
        n = lambda wc: int(config.get("unknowns", {}).get("subsample_n", 0))
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      n={params.n}
      if [ "$n" -le 0 ]; then
        cp -f "{input.unk}" "{output.sub}"
        exit 0
      fi

      PYBIN=$(command -v python || command -v python3 || true)
      if [ -n "$PYBIN" ]; then
        "$PYBIN" - <<'PY' "{input.unk}" "{output.sub}" $n
import sys, random, gzip
src, dst, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
random.seed(100)

def open_any(p, mode):
    return gzip.open(p, mode) if p.endswith(".gz") else open(p, mode)

reservoir = []
count = 0

with open_any(src, "rt") as f:
    while True:
        h = f.readline()
        if not h: break
        s = f.readline(); plus = f.readline(); q = f.readline()
        if not q: break
        rec = (h, s, plus, q)
        count += 1
        if len(reservoir) < n:
            reservoir.append(rec)
        else:
            j = random.randrange(count)
            if j < n:
                reservoir[j] = rec

with open(dst, "w") as out:
    for (h,s,p,q) in reservoir:
        out.write(h); out.write(s); out.write(p); out.write(q)
PY
        exit 0
      fi

      echo "No Python found for subsampling and n>0. Consider installing seqtk or adding Python." >&2
      exit 127
    """

def _unknown_pool_inputs(wc):
    from snakemake.io import expand
    return expand(os.path.join(UNKNOWN_DIR, "{stem}.unknown.fastq.sub.fastq"),
                  stem=_filtered_stems())

# 2.4 pool all subsampled unknowns (now driven by the fan-out)
rule unknown_pool:
    input:
        pool = _unknown_pool_inputs,
        done = rules.itgdb_map_reads.output.done 
    output:
        pool = temp(os.path.join(UNKNOWN_DIR, "unknown_pool.fastq"))
    threads:   Rq("unknown_pool", "threads")
    resources:
        mem_mb    = Rq("unknown_pool", "mem_mb"),
        runtime   = Rq("unknown_pool", "runtime"),
        partition = Rq("unknown_pool", "partition"),
        account   = Rq("unknown_pool", "account"),
        extra     = R("unknown_pool", "extra"),
    params:
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    container: CONTAINERS["cpu"]
    run:
        import shutil
        with open(output[0], "wb") as outfh:
            for f in input.pool:
                with open(f, "rb") as infh:
                    shutil.copyfileobj(infh, outfh, length=1024*1024)

rule unknown_cluster:
    input: pool = rules.unknown_pool.output.pool
    output:
        derep = temp(UNKNOWN_DEREP),
        cent  = UNKNOWN_CENT,
        cent_filt = UNKNOWN_CENT_FILT,
        done = touch(os.path.join(UNKNOWN_DIR, ".cluster.done"))
    threads: Rq("unknown_cluster", "threads")
    resources:
        mem_mb   = Rq("unknown_cluster", "mem_mb"),
        runtime  = Rq("unknown_cluster", "runtime"),
        partition = Rq("unknown_cluster", "partition"),
        account  = Rq("unknown_cluster", "account"),
        extra    = R("unknown_cluster", "extra")
    params:
        container_rev = lambda wc: config["container_rev"].get("cpu","0"),
        cid   = lambda wc: float(config["unknowns"]["id"]),
        minsz = lambda wc: int(config["unknowns"]["min_total_abund"])
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      if ! grep -q '^@' "{input.pool}"; then
        : > "{output.derep}"; : > "{output.cent}"; : > "{output.cent_filt}"
        exit 0
      fi
      command -v vsearch >/dev/null || {{ echo "vsearch not found"; exit 127; }}
      vsearch --fastx_uniques "{input.pool}" \
              --sizeout --relabel UNK_  --threads {threads} \
              --fastaout "{output.derep}"
      if ! grep -q '^>' "{output.derep}"; then
        : > "{output.cent}"; : > "{output.cent_filt}"; exit 0
      fi
      vsearch --cluster_fast "{output.derep}" \
              --id {params.cid} --strand both --sizein --sizeout \
              --centroids "{output.cent}" --threads {threads}
      # Filter by size= in fasta header
      awk '/^>/{{
             if (match($0,/size=([0-9]+)/,m)) {{ keep=(m[1]>={params.minsz}) }} else {{ keep=1 }}
             if (keep) print
           }} !/^>/{{
             if (keep) print
           }}' "{output.cent}" > "{output.cent_filt}"
      
      : > {output.done}
    """
    
def list_unknown_subs(_wc=None):
    import glob, os
    return sorted(glob.glob(os.path.join(UNKNOWN_DIR, "*.unknown.fastq.sub.fastq")))

rule unknown_tbl_per_samp:
    input:
        refs = rules.unknown_cluster.output.cent_filt,
        subs = list_unknown_subs
    output:
        merged = UNKNOWN_TABLE_MERGED
    threads: Rq("unknown_tbl_per_samp", "threads")
    resources:
        mem_mb   = Rq("unknown_tbl_per_samp", "mem_mb"),
        runtime  = Rq("unknown_tbl_per_samp", "runtime"),
        partition = Rq("unknown_tbl_per_samp", "partition"),
        account  = Rq("unknown_tbl_per_samp", "account"),
        extra    = R("unknown_tbl_per_samp", "extra")
    params:
        dir = UNKNOWN_TABLE_DIR,
        id    = lambda wc: float(config.get("unknowns", {}).get("id", 0.70)),
        iddef = lambda wc: int(config.get("unknowns", {}).get("iddef", 1)),
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    container: CONTAINERS["cpu"]
    run:
        import glob, os, csv, subprocess, shlex

        refs = input.refs
        subs = list(input.subs)
        out = output.merged
        tdir = params.dir
        os.makedirs(tdir, exist_ok=True)

        # If no unknown refs, emit empty table
        if (not os.path.exists(refs)) or (os.path.getsize(refs) == 0):
            with open(out, "w", newline="") as fh:
                csv.writer(fh, delimiter="\t").writerow(["OTU"])
            return

        # Generate per-sample tables
        made = []
        for f in subs:
            if not os.path.exists(f) or os.path.getsize(f) == 0:
                continue
            sid = os.path.basename(f).replace(".unknown.fastq.sub.fastq","")
            tab = os.path.join(tdir, f"otu_unknown_{sid}.tsv")
            cmd = f'vsearch --usearch_global {shlex.quote(f)} --db {shlex.quote(refs)} ' \
                  f'--id {params.id} --iddef {params.iddef} --strand both --qmask none ' \
                  f'--otutabout {shlex.quote(tab)} --threads {threads}'
            subprocess.run(cmd, shell=True, check=True)
            if os.path.exists(tab) and os.path.getsize(tab) > 0:
                made.append(tab)

        if not made:
            with open(out, "w", newline="") as fh:
                csv.writer(fh, delimiter="\t").writerow(["OTU"])
            return

        # read all per-sample tables into dict-of-dicts
        sample_names = []
        data = {} 
        for tab in sorted(made):
            sid = os.path.basename(tab)
            if sid.startswith("otu_unknown_"): sid = sid[len("otu_unknown_"):]
            if sid.endswith(".tsv"): sid = sid[:-4]
            sample_names.append(sid)

            with open(tab, newline="") as fh:
                r = csv.reader(fh, delimiter="\t")
                header = next(r, None)
                for row in r:
                    if not row: continue
                    if row[0].startswith("#"): continue
                    if row[0].upper() in ("OTU","OTUID","OTU_ID","OTUID"): continue
                    otu = row[0]
                    val = 0
                    for x in row[1:]:
                        try:
                            s = str(x).strip()
                            if s and s.upper() != "NA":
                                val += int(float(s))
                        except Exception:
                            pass
                    data.setdefault(otu, {})
                    data[otu][sid] = data[otu].get(sid, 0) + val

        # write merged table
        with open(out, "w", newline="") as fh:
            w = csv.writer(fh, delimiter="\t")
            w.writerow(["OTU"] + sample_names)
            for otu in sorted(data.keys()):
                w.writerow([otu] + [data[otu].get(s, 0) for s in sample_names])
                
rule itgdb_species_tables:
    input:
        done   = rules.itgdb_map_reads.output.done, 
        taxmap = rules.itgdb_taxmap.output.taxmap
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
from collections import Counter, defaultdict

taxmap_fn     = r"{input.taxmap}"
species_merge = r"{output.merged}"

species_dir = os.path.dirname(species_merge)          
itgdb_dir   = os.path.dirname(species_dir)            
genus_dir   = os.path.join(itgdb_dir, "genus")
paf_dir     = itgdb_dir                              
os.makedirs(species_dir, exist_ok=True)
os.makedirs(genus_dir,   exist_ok=True)

mapq_min   = int("{params.mapq_min}")
aln_min_bp = int("{params.aln_min_bp}")

id2tax = dict()
with open(taxmap_fn, "r", encoding="utf-8", errors="ignore") as fh:
    r = csv.reader(fh, delimiter="\t")
    first = next(r, None)
    if first and (first[0].lower() in ("id", "#id", "#")):
        pass
    else:
        if first:  # first line is data
            if len(first) >= 2:
                id2tax[first[0]] = first[1]
        for row in r:
            if row and not row[0].startswith("#") and len(row) >= 2:
                id2tax[row[0]] = row[1]
    if not id2tax:
        fh.seek(0)
        for row in csv.reader(fh, delimiter="\t"):
            if row and not row[0].startswith("#") and len(row) >= 2:
                id2tax[row[0]] = row[1]

rx = {{
    "k": re.compile(r"k__([^;|,\s]+)"),
    "p": re.compile(r"p__([^;|,\s]+)"),
    "c": re.compile(r"c__([^;|,\s]+)"),
    "o": re.compile(r"o__([^;|,\s]+)"),
    "f": re.compile(r"f__([^;|,\s]+)"),
    "g": re.compile(r"g__([^;|,\s]+)"),
    "s": re.compile(r"s__([^;|,\s]+)")
}}

def _norm_target_key(tname):
    return re.split(r"[|\s]", tname)[0]

def _get(rxk, tax):
    m = rx[rxk].search(tax or "")
    return (m.group(1).replace("_"," ").strip() if m else None)

def parse_ranks(tax):
    if not tax or tax == "NA":
        return dict(k="Unassigned", p="Unassigned", c="Unassigned",
                    o="Unassigned", f="Unassigned", g="Unassigned", s="Unassigned")
    k = _get("k", tax) or "Unassigned"
    p = _get("p", tax) or "Unassigned"
    c = _get("c", tax) or "Unassigned"
    o = _get("o", tax) or "Unassigned"
    f = _get("f", tax) or "Unassigned"
    g = _get("g", tax) or "Unassigned"
    s = _get("s", tax)
    if not s:
        s = (g + " sp.") if g and g != "Unassigned" else "Unassigned"
    return dict(k=k, p=p, c=c, o=o, f=f, g=g, s=s)
  
pafs = sorted(glob.glob(os.path.join(paf_dir, "*.paf")))
if not pafs:
    with open(species_merge, "w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, delimiter="\t").writerow(["Kingdom","Phylum","Class","Order","Family","Genus","Species"])
    with open(os.path.join(genus_dir, "itgdb_genus_merged.tsv"), "w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, delimiter="\t").writerow(["Genus"])
    sys.exit(0)
    
samples = []
species_counts_by_sample = defaultdict(Counter)  
genus_counts_by_sample   = defaultdict(Counter) 
species_tax_tuple_counts = defaultdict(Counter)  
genus_tax_tuple_counts   = defaultdict(Counter)

for paf in pafs:
    sid = os.path.basename(paf)[:-4]
    samples.append(sid)

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
            rk  = parse_ranks(tax)
            sp  = rk["s"]; ge = rk["g"]

            species_counts_by_sample[sid][sp] += 1
            genus_counts_by_sample[sid][ge]   += 1

            tax_tuple = (rk["k"], rk["p"], rk["c"], rk["o"], rk["f"], rk["g"], rk["s"])
            species_tax_tuple_counts[sp][tax_tuple] += 1

            genus_tuple = (rk["k"], rk["p"], rk["c"], rk["o"], rk["f"], rk["g"])
            genus_tax_tuple_counts[ge][genus_tuple] += 1
            
for sid in samples:
    sp_path = os.path.join(species_dir, "itgdb_species_" + sid + ".tsv")
    with open(sp_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["Species", sid])
        for sp, c in species_counts_by_sample[sid].most_common():
            w.writerow([sp, c])

    ge_path = os.path.join(genus_dir, "itgdb_genus_" + sid + ".tsv")
    with open(ge_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["Genus", sid])
        for ge, c in genus_counts_by_sample[sid].most_common():
            w.writerow([ge, c])

samples = sorted(samples)
all_genera = sorted(set().union(*(set(genus_counts_by_sample[s].keys()) for s in samples))) if samples else []
genus_merge = os.path.join(genus_dir, "itgdb_genus_merged.tsv")
with open(genus_merge, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh, delimiter="\t")
    w.writerow(["Genus"] + samples)
    for ge in all_genera:
        w.writerow([ge] + [genus_counts_by_sample[s].get(ge, 0) for s in samples])
        

species2tuple = {{}}
for sp, cnts in species_tax_tuple_counts.items():
    species2tuple[sp] = cnts.most_common(1)[0][0]  
    
genus2tuple = {{}}
for ge, cnts in genus_tax_tuple_counts.items():
    genus2tuple[ge] = cnts.most_common(1)[0][0] if cnts else ("Unassigned",)*5 + (ge or "Unassigned",)

all_species = sorted(set().union(*(set(species_counts_by_sample[s].keys()) for s in samples))) if samples else []
all_genera = sorted(set().union(*(set(genus_counts_by_sample[s].keys()) for s in samples))) if samples else []
genus_merge = os.path.join(genus_dir, "itgdb_genus_merged.tsv")

with open(species_merge, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh, delimiter="\t")
    w.writerow(["Kingdom","Phylum","Class","Order","Family","Genus","Species"] + samples)
    for sp in all_species:
        k,p,c,o,f,g,_ = species2tuple.get(sp, ("Unassigned",)*6 + (sp,))
        row_counts = [species_counts_by_sample[s].get(sp, 0) for s in samples]
        w.writerow([k,p,c,o,f,g,sp] + row_counts)
        
with open(genus_merge, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh, delimiter="\t")
    # NEW header with higher ranks
    w.writerow(["Kingdom","Phylum","Class","Order","Family","Genus"] + samples)
    for ge in all_genera:
        k,p,c,o,f,g = genus2tuple.get(ge, ("Unassigned","Unassigned","Unassigned","Unassigned","Unassigned", ge))
        row_counts = [genus_counts_by_sample[s].get(ge, 0) for s in samples]
        w.writerow([k,p,c,o,f,g] + row_counts)
PY
    """

rule pooled_concat_reads:
    input:
        reads = rules.fastcat_filter.output.fastq
    output:
        pooled = POOLED_FQ
    threads: Rq("pooled_concat_reads", "threads")
    resources:
        mem_mb    = Rq("pooled_concat_reads", "mem_mb"), 
        runtime   = Rq("pooled_concat_reads", "runtime"),
        partition = Rq("pooled_concat_reads", "partition"),
        account   = Rq("pooled_concat_reads", "account"),
        extra     =  R("pooled_concat_reads", "extra"),
    log: os.path.join(OUT, "logs/pooled_concat_reads.log")
    params:
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p "$(dirname "{output.pooled}")"
      : > "{output.pooled}"
      shopt -s nullglob
      for fq in "{input.reads}"/*.fastq "{input.reads}"/*.fastq.gz; do
        [ -e "$fq" ] || continue
        if [[ "$fq" == *.gz ]]; then gzip -dc "$fq" >> "{output.pooled}"; else cat "$fq" >> "{output.pooled}"; fi
      done
      grep -q '^@' "{output.pooled}" || {{ echo "[pooled_concat_reads] no FASTQ records"; : > "{output.pooled}"; }}
    """
    
rule isonclust3_pooled:
    input: rules.pooled_concat_reads.output.pooled
    output: 
        pooled = directory(POOLED_OTU_DIR),
        done = touch(os.path.join(POOLED_OTU_DIR, ".done"))
    threads: Rq("isonclust3_pooled", "threads")
    resources:
        mem_mb    = Rq("isonclust3_pooled", "mem_mb"), 
        runtime   = Rq("isonclust3_pooled", "runtime"),
        partition = Rq("isonclust3_pooled", "partition"),
        account   = Rq("isonclust3_pooled", "account"),
        extra     =  R("isonclust3_pooled", "extra"),
    log: os.path.join(OUT, "logs/isonclust3.log")
    params:
        extra         = lambda wc: config.get("pooled",{}).get("isonclust3_extra",""),
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      exec > "{log}" 2>&1
      set -x
      
      mkdir -p "{output.pooled}"
      find "{output.pooled}" -mindepth 1 -maxdepth 1 -exec rm -rf {{}} + || true

      if ! grep -q '^@' "{input}"; then
        echo "[isonclust3_pooled] empty pooled FASTQ" >&2
        : > "{output.done}"
        exit 0
      fi
      
      RAYON_NUM_THREADS={threads} \
      isONclust3 \
        --fastq "{input}" \
        --outfolder "{output.pooled}" \
        --mode ont \
        --post-cluster \
        --verbose  {params.extra}

      touch "{output.done}"
    """
    
rule spoa_consensus_pooled:
    input: 
        pooled = rules.isonclust3_pooled.output.pooled,
        done   = rules.isonclust3_pooled.output.done
    output:
        done = os.path.join(POOLED_CONS_DIR, ".done")
    threads: Rq("spoa_consensus_pooled", "threads")
    resources:
        mem_mb    = Rq("spoa_consensus_pooled", "mem_mb"), 
        runtime   = Rq("spoa_consensus_pooled", "runtime"),
        partition = Rq("spoa_consensus_pooled", "partition"),
        account   = Rq("spoa_consensus_pooled", "account"),
        extra     =  R("spoa_consensus_pooled", "extra"),
    log: os.path.join(OUT, "logs/spoa_consensus_pooled.log")
    params:
        outdir     = POOLED_CONS_DIR,
        max_reads  = int(config.get("spoa_max_reads", 500)),
        min_reads  = int(config.get("spoa_min_reads", 3)),
        extra      = lambda wc: config.get("spoa_extra",""),
        container_rev = lambda wc: config["container_rev"].get("cpu","0"),
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      mkdir -p "{params.outdir}" "$(dirname "{log}")"
      exec > "{log}" 2>&1

      shopt -s nullglob
      mapfile -t fq_list < <(find "{input.pooled}" -type f -path "*/clustering/fastq_files/*.fastq" | sort)

      for fq in "${{fq_list[@]}}"; do
        sample=$(basename "$(dirname "$(dirname "$fq")")")
        cid=$(basename "$fq" .fastq)
        out="{params.outdir}/${{sample}}_${{cid}}.fasta"

        [[ -s "$out" ]] && {{ echo "skip $out"; continue; }}

        n=$(awk 'END{{print int(NR/4)}}' "$fq")
        (( n < {params.min_reads} )) && {{ echo "skip (too few reads: $n) → $out"; continue; }}

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
        container_rev = lambda wc: config["container_rev"].get("cpu","0"),
        extra = lambda wc: config.get("isonclust3_extra","") 
    container: CONTAINERS["cpu"]
    shell: r"""
      set -euo pipefail
      exec > "{log}" 2>&1
      set -x
      
      mkdir -p "{output}"
      find "{output}" -mindepth 1 -maxdepth 1 -exec rm -rf {{}} + || true

      shopt -s nullglob
      for fq in {input}/*.fastq; do
        samp=$(basename "$fq" .fastq)
        outdir="{output}/${{samp}}"
        mkdir -p "$outdir"
        
        [[ -s "$fq" ]] || {{ echo "skip $fq (empty)"; continue; }}
        head -n1 "$fq" | grep -q '^@' || {{ echo "skip $fq (not FASTQ?)"; continue; }}
        
        RAYON_NUM_THREADS={threads} \
        isONclust3 \
          --fastq "$fq" \
          --outfolder "$outdir" \
          --mode ont  \
          --post-cluster \
          --verbose {params.extra}
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

def _consensus_dir():
    return POOLED_CONS_DIR if enabled_pooled() else os.path.join(TMP, "consensus_drafts")

rule vsearch_pool_cluster: 
    input:
      done = rules.spoa_consensus_pooled.output.done if enabled_pooled() else rules.spoa_consensus.output.done
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
        consdir    = lambda wc: _consensus_dir(),
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
                --id {params.id_primary} --strand both --qmask none \
                --centroids "{output.cent99}" --threads {threads}
      
        vsearch --cluster_fast "{params.pooldir}/otus_derep.fasta" \
                --id {params.id_legacy}  --strand both --qmask none \
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
    params:
        container_rev = lambda wc: config["container_rev"].get("cpu","0")
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
        refs  = rules.uniqify_otu_centroids.output.cent97_uniq 
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
      mkdir -p "$(dirname "{output.all_reads}")" "$(dirname "{output.bam}")" "$(dirname "{log}")"
      exec > "{log}" 2>&1
      set -x
      
      tmp_raw=$(mktemp)
      tmp_clean=$(mktemp)
      trap 'rm -f "$tmp_raw" "$tmp_clean"' EXIT
      
      shopt -s nullglob
      mapfile -d '' -t FQS < <(find "{input.reads}" -maxdepth 1 -type f \
         \( -name '*.fastq' -o -name '*.fastq.gz' \) -print0 | sort -z)

      : > "$tmp_raw"
      for f in "${{FQS[@]}}"; do
        if [[ "$f" == *.gz ]]; then
          gzip -dc -- "$f"
        else
          cat -- "$f"
        fi | sed -e '$a\' >> "$tmp_raw"
      done

      if [[ ! -s "$tmp_raw" ]] || ! grep -q '^@' "$tmp_raw"; then
        echo "[map_all_reads] No reads after pooling; emitting header-only BAM." >&2
        samtools faidx "{input.refs}"
        awk 'BEGIN{{OFS="\t"}} {{print "@SQ\tSN:"$1"\tLN:"$2}}' "{input.refs}.fai" > "{output.bam}.header.sam"
        samtools view -b -o "{output.bam}" "{output.bam}.header.sam"
        rm -f "{output.bam}.header.sam"
        samtools index -@ {threads} "{output.bam}"
        : > "{output.all_reads}"
        exit 0
      fi
      
      awk '
        NR%4==1{{h=$0}}
        NR%4==2{{s=$0;ls=length($0)}}
        NR%4==3{{p=$0}}
        NR%4==0{{lq=length($0); if (substr(h,1,1)=="@" && substr(p,1,1)=="+" && lq==ls){{print h RS s RS p RS $0}} else {{bad++}}}}
        END{{ if(bad>0) printf("[map_all_reads] dropped %d malformed reads\n",bad) > "/dev/stderr" }}
      ' "$tmp_raw" > "$tmp_clean"

      if [[ $(awk 'END{{print NR%4}}' "$tmp_clean") -ne 0 ]]; then
        echo "[map_all_reads] Pooled FASTQ not multiple of 4 lines after cleaning." >&2
        exit 1
      fi

      mv -f "$tmp_clean" "{output.all_reads}"

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
        max_medaka_reads  = int(config.get("max_medaka_reads", 0)),
        medaka_batch  = int(config.get("medaka_batch", 100)),
        polish_dir    = POLISH_DIR  
    container:
        CONTAINERS["gpu"]
    shell: r"""
        set -euo pipefail
        export OMP_NUM_THREADS=1
        ulimit -n 8192
        
        TMPBASE="${{SLURM_TMPDIR:-${{TMPDIR:-/tmp}}}}"
        workdir="$(mktemp -d "$TMPBASE/medaka.XXXXXX")"
        trap 'rm -rf "$workdir"' EXIT

        outdir="$workdir/medaka_refined"
        mkdir -p "$outdir"
        
        CLEAN_IN="{input.reads}"
        tmp_sub=""
        trap '[[ -n "$tmp_sub" ]] && rm -f "$tmp_sub"' EXIT

        if (( {params.max_medaka_reads} > 0 )); then
          nreads=$(awk 'END{{print int(NR/4)}}' "{input.reads}")
          if (( nreads > {params.max_medaka_reads} )); then
            k=$(( (nreads + {params.max_medaka_reads} - 1) / {params.max_medaka_reads} ))
            tmp_sub=$(mktemp)
            awk -v k="$k" '
              {{
                b=(NR-1)%4
                if (b==0) {{ r=int((NR-1)/4); keep=(r%k==0) }}
                if (keep) print
              }}
            ' "{input.reads}" > "$tmp_sub"
            CLEAN_IN="$tmp_sub"
            echo "[medaka_polish] downsampled reads: original=" "$nreads" " keep≈" {params.max_medaka_reads} " (k=" "$k" ")" >&2
          fi
        fi
        
        cp "{input.draft}" "$workdir/draft.fasta"
        samtools faidx "$workdir/draft.fasta"
        minimap2 -d "$workdir/draft.fasta.map-ont.mmi" "$workdir/draft.fasta"
        
        export MEDAKA_TMPDIR="$workdir"

        medaka_consensus \
          -i "$CLEAN_IN" \
          -d "$workdir/draft.fasta" \
          -o "$outdir" \
          -m "{params.medaka_model}" \
          -t {threads} \
          -b {params.medaka_batch} \
          --bacteria

        cp "$outdir/consensus.fasta" "{output.polished}"
        """

##############################################
#  POST-POLISH OTU PROCESSING STAGE
#  (Runs after medaka_polish)
##############################################

# 1. Identify chimeras + assign taxonomy
rule chimera_taxonomy:
    input:
        fasta = rules.medaka_polish.output.polished,
        db    = ancient(ITGDB_UDB)
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

      vsearch --uchime3_denovo "{input.fasta}" \
              --nonchimeras "{output.nonchim}" \
              --chimeras    "{output.chimera}" \
              --mindiv 3 --minh 0.28 \
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

rule otu_taxonomy_best:
    input: sintax = rules.chimera_taxonomy.output.sintax
    output: tax = OTU_TAX_BEST
    threads:   Rq("otu_taxonomy_best", "threads")
    resources:
        mem_mb    = Rq("otu_taxonomy_best", "mem_mb"),
        runtime   = Rq("otu_taxonomy_best", "runtime"),
        partition = Rq("otu_taxonomy_best", "partition"),
        account   = Rq("otu_taxonomy_best", "account"),
        extra     =  R("otu_taxonomy_best", "extra"),
    container: CONTAINERS["cpu"]
    run:
        import csv, re
        def best_label(tax, cutoff=0.0):
            if not tax: return "Unassigned"
            s = tax.strip()
            # try species
            m = re.search(r"s__([^,;| ]+)", s)
            if m: return m.group(1).replace("_"," ").strip()
            # genus
            m = re.search(r"g__([^,;| ]+)", s)
            if m:
                g = m.group(1).replace("_"," ").strip()
                return f"{g} sp."
            # family→order→class→phylum→domain
            for rank in ["f","o","c","p","k","d"]:
                m = re.search(fr"{rank}__([^,;| ]+)", s)
                if m:
                    r = m.group(1).replace("_"," ").strip()
                    return f"{r} sp."
            return "Unassigned"

        with open(input.sintax, newline="", encoding="utf-8", errors="ignore") as fh, \
             open(output.tax, "w", newline="", encoding="utf-8") as fo:
            r = csv.reader(fh, delimiter="\t")
            w = csv.writer(fo, delimiter="\t")
            w.writerow(["OTU","taxonomy","label_best"])
            for row in r:
                if not row: continue
                otu = row[0]
                tax = row[1] if len(row)>1 else ""
                w.writerow([otu, tax, best_label(tax)])
                
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
        extra     = R( "otu_alignment", "extra")
    params:
        container_rev = lambda wc: config["container_rev"].get("tree","0")
    container: CONTAINERS["tree"]
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
        tree = os.path.join(OUT, "otu/otu_tree.treefile"),
        done =  touch(os.path.join(OUT, "otu", ".tree.done"))
    threads: Rq("iqtree3_tree", "threads")
    resources:
        mem_mb    = Rq("iqtree3_tree", "mem_mb"),
        runtime   = Rq("iqtree3_tree", "runtime"),
        partition = Rq("iqtree3_tree", "partition"),
        account   = Rq("iqtree3_tree", "account"),
        extra     = R("iqtree3_tree", "extra")
    log: os.path.join(OUT, "logs/iqtree3_tree.log")
    params:
        container_rev = lambda wc: config["container_rev"].get("tree","0"),
        out_dir       = OUT
    container: CONTAINERS["tree"]
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
      : > {output.done}
    """
    
# 4. Per-sample read counts for each OTU
rule otu_table_per_sample:
    input:
        refs  = rules.collapse_ultraclose.output.fasta,
        reads = rules.fastcat_filter.output.fastq,
        deps_done = rules.fastcat_filter.output.done
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
        map_id        = lambda wc: float(config.get("map_id", 0.90)),
        strand        = lambda wc: config.get("strand", "both"),
        iddef         = lambda wc: int(config.get("otu_mapping", {}).get("iddef", 1)),
        qcov          = lambda wc: float(config.get("otu_mapping", {}).get("qcov", 0.90)),
        tcov          = lambda wc: float(config.get("otu_mapping", {}).get("tcov", 0.90)),
        qmask         = lambda wc: str(config.get("otu_mapping", {}).get("qmask", "none")),
        tables_dir    = os.path.join(OUT, "otu", "tables")
    container: CONTAINERS["cpu"]
    shell:
      r"""
      set -euo pipefail
      mkdir -p "{params.tables_dir}"
      shopt -s nullglob

      PYBIN=$(command -v python || command -v python3 || true)
      [ -n "$PYBIN" ] || {{ echo "No python interpreter found."; exit 127; }}
      
      [[ {params.qcov} == 0.0 ]] && echo "warning: qcov=0 disables coverage filtering" >&2

      for fq in "{input.reads}"/*.fastq; do
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

rule merge_known_unknown:
    input:
        known = rules.otu_table_per_sample.output.merged,
        unk   = rules.unknown_tbl_per_samp.output.merged if enabled_unknowns() else rules.otu_table_per_sample.output.merged
    output:
        merged = OTU_TABLE_ALL
    threads: Rq("merge_known_unknown", "threads")
    resources:
        mem_mb    = Rq("merge_known_unknown", "mem_mb"),
        runtime   = Rq("merge_known_unknown", "runtime"),
        partition = Rq("merge_known_unknown", "partition"),
        account   = Rq("merge_known_unknown", "account"),
        extra     =  R("merge_known_unknown", "extra")
    container: CONTAINERS["cpu"]
    run:
        import os, csv
        import collections

        known = input.known
        unk = input.unk
        out = output.merged

        def load_tsv(path):
            rows = []
            with open(path, newline="") as fh:
                for i, row in enumerate(csv.reader(fh, delimiter="\t")):
                    rows.append(row)
            return rows

        if not os.path.exists(unk):
            # just copy known
            import shutil
            shutil.copy2(known, out)
            return

        K = load_tsv(known)
        U = load_tsv(unk)

        # If either is header-only, handle simply
        if len(U) <= 1:
            import shutil
            shutil.copy2(known, out); return
        if len(K) <= 1:
            import shutil
            shutil.copy2(unk, out); return

        k_header, *k_rows = K
        u_header, *u_rows = U
        k_samples = k_header[1:]
        u_samples = u_header[1:]
        all_samples = k_samples[:]  # assume same names since both came from filtered FASTQs
        # If sample sets differ, union them
        for s in u_samples:
            if s not in all_samples: all_samples.append(s)

        # maps
        def build_map(rows):
            m = {}
            for r in rows:
                if not r: continue
                m[r[0]] = { s:int(v) if (v and v!='NA') else 0 for s, v in zip(all_samples, r[1:] + [0]*(len(all_samples)-len(r)+1)) }
            return m

        km = build_map(k_rows)
        um = build_map(u_rows)

        # merge
        otus = sorted(set(km.keys()) | set(um.keys()))
        with open(out, "w", newline="") as fh:
            w = csv.writer(fh, delimiter="\t")
            w.writerow(["OTU"] + all_samples)
            for o in otus:
                row = [o] + [ (km.get(o,{}).get(s,0) + um.get(o,{}).get(s,0)) for s in all_samples ]
                w.writerow(row)

rule apply_taxonomy_filter:
    input:
        table = rules.merge_known_unknown.output.merged,            # counts with unknowns
        tax   = rules.otu_taxonomy_best.output.tax
    output:
        final = OTU_TABLE_FINAL
    threads:   Rq("apply_taxonomy_filter", "threads")
    resources:
        mem_mb    = Rq("apply_taxonomy_filter", "mem_mb"),
        runtime   = Rq("apply_taxonomy_filter", "runtime"),
        partition = Rq("apply_taxonomy_filter", "partition"),
        account   = Rq("apply_taxonomy_filter", "account"),
        extra     =  R("apply_taxonomy_filter", "extra"),
    container: CONTAINERS["cpu"]
    params:
        enable = lambda wc: bool(config.get("taxonomy_filter",{}).get("drop_euk_chl_mito", False)),
        pats   = lambda wc: list(config.get("taxonomy_filter",{}).get("patterns", []))
    run:
        import csv, re
        enable = params.enable
        pats = params.pats
        to_drop = set()
        if enable and pats and os.path.exists(input.tax):
            rx = re.compile("|".join([re.escape(p) for p in pats]), flags=re.I)
            with open(input.tax, newline="", encoding="utf-8", errors="ignore") as fh:
                r = csv.DictReader(fh, delimiter="\t")
                for row in r:
                    tax = row.get("taxonomy","") or ""
                    if rx.search(tax):
                        to_drop.add(row["OTU"])

        with open(input.table, newline="", encoding="utf-8") as fin, \
             open(output.final, "w", newline="", encoding="utf-8") as fout:
            rr = csv.reader(fin, delimiter="\t")
            w  = csv.writer(fout, delimiter="\t")
            header = next(rr, None)
            if header: w.writerow(header)
            for row in rr:
                if not row: continue
                otu = row[0]
                if enable and otu in to_drop:
                    continue
                w.writerow(row)

    
rule iqtree3_model_extract:
    input:
        iq = os.path.join(OUT, "otu/otu_tree.iqtree")
    output:
        model = os.path.join(OUT, "otu/otu_tree.model.txt")
    threads: Rq("iqtree3_model_extract", "threads")
    resources:
        mem_mb    = Rq("iqtree3_model_extract", "mem_mb"),
        runtime   = Rq("iqtree3_model_extract", "runtime"),
        partition = Rq("iqtree3_model_extract", "partition"),
        account   = Rq("iqtree3_model_extract", "account"),
        extra     = R( "iqtree3_model_extract", "extra")
    log: os.path.join(OUT, "logs/iqtree3_model_extract.log")
    container: CONTAINERS["tree"]
    run:
        import re, io
        model = "GTR+G"   
        with open(input.iq, "r", encoding="utf-8", errors="ignore") as fh:
            txt = fh.read()
        m = re.search(r"(Best-fit model:|Model of substitution:)\s*([A-Za-z0-9+._-]+)", txt)
        if m: model = m.group(2).strip()
        with open(output.model, "w") as fo:
            fo.write(model + "\n")
            
rule align_unknowns_to_backbone:
    input:
        backbone = os.path.join(OUT, "otu/otu_references_aligned.fasta"),
        unk      = UNKNOWN_CENT_FILT,
        done     = rules.unknown_cluster.output.done
    output:
        q_aln = os.path.join(OUT, "otu/unknowns_aligned.fasta")
    threads: Rq("align_unknowns_to_backbone", "threads")
    resources:
        mem_mb    = Rq("align_unknowns_to_backbone", "mem_mb"),
        runtime   = Rq("align_unknowns_to_backbone", "runtime"),
        partition = Rq("align_unknowns_to_backbone", "partition"),
        account   = Rq("align_unknowns_to_backbone", "account"),
        extra     = R( "align_unknowns_to_backbone", "extra")
    log: os.path.join(OUT, "logs/align_unknowns_to_backbone.log")
    container: CONTAINERS["tree"]
    shell: r"""
      set -euo pipefail
      if ! grep -q '^>' "{input.unk}"; then
        : > "{output.q_aln}"
        exit 0
      fi
      command -v mafft >/dev/null || {{ echo "mafft not found"; exit 127; }}
      mafft --addfragments "{input.unk}" --keeplength --reorder --thread {threads} "{input.backbone}" > "{output.q_aln}"
    """
    
rule epa_place_unknowns:
    input:
        backbone_msa = os.path.join(OUT, "otu/otu_references_aligned.fasta"),
        backbone_tree = os.path.join(OUT, "otu/otu_tree.treefile"),
        q_aln = rules.align_unknowns_to_backbone.output.q_aln,
        model = rules.iqtree3_model_extract.output.model
    output:
        jplace = os.path.join(OUT, "otu/unknowns.placements.jplace")
    threads: Rq("epa_place_unknowns", "threads")
    resources:
        mem_mb    = Rq("epa_place_unknowns", "mem_mb"),
        runtime   = Rq("epa_place_unknowns", "runtime"),
        partition = Rq("epa_place_unknowns", "partition"),
        account   = Rq("epa_place_unknowns", "account"),
        extra     = R( "epa_place_unknowns", "extra")
    log: os.path.join(OUT, "logs/epa_place_unknowns.log")
    container: CONTAINERS["tree"]
    params:
        extra = ""  
    shell: r"""
      set -euo pipefail
      if ! grep -q '^>' "{input.q_aln}"; then
        echo '{{"tree": "", "placements": []}}' > "{output.jplace}"
        exit 0
      fi
      command -v epa-ng >/dev/null || {{ echo "epa-ng not found"; exit 127; }}
      mdl=$(cat "{input.model}" | tr -d '\n\r')
      epa-ng \
        --ref-msa "{input.backbone_msa}" \
        --tree    "{input.backbone_tree}" \
        --query   "{input.q_aln}" \
        --model   "$mdl" \
        --threads {threads} {params.extra} \
        --outdir  "$(dirname "{output.jplace}")"/epa_tmp
      mv -f "$(dirname "{output.jplace}")"/epa_tmp/epa_result.jplace "{output.jplace}"
      rm -rf "$(dirname "{output.jplace}")"/epa_tmp || true
    """
    
rule graft_unknowns_on_tree:
    input:
        tree   = os.path.join(OUT, "otu/otu_tree.treefile"),
        jplace = rules.epa_place_unknowns.output.jplace
    output:
        tree_grafted = os.path.join(OUT, "otu/otu_tree.with_unknowns.nwk")
    threads: Rq("graft_unknowns_on_tree", "threads")
    resources:
        mem_mb    = Rq("graft_unknowns_on_tree", "mem_mb"),
        runtime   = Rq("graft_unknowns_on_tree", "runtime"),
        partition = Rq("graft_unknowns_on_tree", "partition"),
        account   = Rq("graft_unknowns_on_tree", "account"),
        extra     = R( "graft_unknowns_on_tree", "extra")
    log: os.path.join(OUT, "logs/graft_unknowns_on_tree.log")
    container: CONTAINERS["tree"]
    shell: r"""
      set -euo pipefail
      if ! command -v gappa >/dev/null; then
        echo "gappa not found" >&2; exit 127
      fi
      outdir="$(dirname "{output.tree_grafted}")/gappa_out"
      rm -rf "$outdir"
      gappa graft \
        --jplace "{input.jplace}" \
        --reference-tree "{input.tree}" \
        --out-dir "$outdir" \
        --allow-file-overwriting
      mv -f "$outdir"/grafted.tre "{output.tree_grafted}"
      rm -rf "$outdir" || true
    """                
                
rule sync_exports_for_R:
    input:
        table = os.path.join(OUT, "otu/otu_table_final.tsv"),
        tax   = os.path.join(OUT, "otu/otu_taxonomy_best.tsv"),
        fasta_known = os.path.join(OUT, "otu/otus_ultraclose_merged.fasta"),
        fasta_unknown = UNKNOWN_CENT_FILT,
        tree = rules.graft_unknowns_on_tree.output.tree_grafted
    output:
        table_sync = os.path.join(OUT, "otu/otu_table_final.synced.tsv"),
        fasta_sync = os.path.join(OUT, "otu/otu_refseq.synced.fasta"),
        tree_sync  = os.path.join(OUT, "otu/otu_tree.synced.nwk")
    threads: Rq("sync_exports_for_R", "threads")
    resources:
        mem_mb    = Rq("sync_exports_for_R", "mem_mb"),
        runtime   = Rq("sync_exports_for_R", "runtime"),
        partition = Rq("sync_exports_for_R", "partition"),
        account   = Rq("sync_exports_for_R", "account"),
        extra     = R( "sync_exports_for_R", "extra")
    log: os.path.join(OUT, "logs/sync_exports_for_R.log")
    container: CONTAINERS["cpu"]
    run:
        from Bio import Phylo, SeqIO
        import csv, io, sys, os

        # 1) read tree tips
        tr = Phylo.read(input.tree, "newick")
        tips = [t.name for t in tr.get_terminals() if t.name]

        # 2) filter OTU table to tree tips (preserve column order)
        with open(input.table, newline="") as fh:
            rows = list(csv.reader(fh, delimiter="\t"))
        header, body = rows[0], rows[1:]
        keep = []
        for r in body:
            if r and r[0] in tips:
                keep.append(r)
        with open(output.table_sync, "w", newline="") as fo:
            w = csv.writer(fo, delimiter="\t")
            w.writerow(header)
            w.writerows(sorted(keep, key=lambda x: x[0]))

        # 3) build mapping of sequences from known + unknown
        seqs = {}
        for fn in (input.fasta_known, input.fasta_unknown):
            if os.path.exists(fn) and os.path.getsize(fn) > 0:
                for rec in SeqIO.parse(fn, "fasta"):
                    # keep simple OTU ID before space/pipe if present
                    rid = str(rec.id).split()[0].split("|")[0]
                    seqs[rid] = rec.seq

        # 4) write FASTA only for OTUs that remain in synced table
        keep_ids = set(r[0] for r in keep)
        with open(output.fasta_sync, "w") as fo:
            for oid in sorted(keep_ids):
                if oid in seqs:
                    fo.write(f">{oid}\n{str(seqs[oid])}\n")

        # 5) prune tree to synced tips (defensive) and write
        tipset = set(keep_ids)
        for cl in list(tr.get_terminals()):
            if cl.name not in tipset:
                tr.prune(target=cl)
        Phylo.write(tr, output.tree_sync, "newick")
                
