#!/usr/bin/env python3
import os, sys, csv, traceback
from Bio import Phylo, SeqIO

# Snakemake I/O
inp = snakemake.input
out = snakemake.output
log = snakemake.log[0]

os.makedirs(os.path.dirname(log), exist_ok=True)
with open(log, "a") as L:
    print("[sync] starting", file=L)
    print(f"[sync] python: {sys.executable}", file=L)
    print(f"[sync] sys.version: {sys.version}", file=L)
    for p in ("table","fasta_known","fasta_unknown","tree"):
        fn = str(inp[p])
        print(f"[sync] check {p}: exists={os.path.exists(fn)} size={os.path.getsize(fn) if os.path.exists(fn) else 'NA'}", file=L)

try:
    # 1) read tree tips
    tr = Phylo.read(inp.tree, "newick")
    tips = [t.name for t in tr.get_terminals() if t.name]

    # 2) filter OTU table to tips
    with open(inp.table, newline="") as fh:
        rows = list(csv.reader(fh, delimiter="\t"))
    header, body = rows[0], rows[1:]
    keep = [r for r in body if r and r[0] in tips]

    os.makedirs(os.path.dirname(out.table_sync), exist_ok=True)
    with open(out.table_sync, "w", newline="") as fo:
        w = csv.writer(fo, delimiter="\t")
        w.writerow(header)
        w.writerows(sorted(keep, key=lambda x: x[0]))

    # 3) map sequences from known + unknown
    seqs = {}
    for fn in (inp.fasta_known, inp.fasta_unknown):
        if os.path.exists(fn) and os.path.getsize(fn) > 0:
            for rec in SeqIO.parse(fn, "fasta"):
                rid = str(rec.id).split()[0].split("|")[0]
                seqs[rid] = rec.seq

    # 4) write FASTA for kept IDs
    keep_ids = {r[0] for r in keep}
    with open(out.fasta_sync, "w") as fo:
        for oid in sorted(keep_ids):
            if oid in seqs:
                fo.write(f">{oid}\n{str(seqs[oid])}\n")

    # 5) prune tree to kept tips
    tipset = keep_ids
    for cl in list(tr.get_terminals()):
        if cl.name not in tipset:
            tr.prune(target=cl)
    Phylo.write(tr, out.tree_sync, "newick")

    with open(log, "a") as L:
        print(f"[sync] kept {len(keep)} OTUs; wrote:", file=L)
        print(f"  table_sync = {out.table_sync}", file=L)
        print(f"  fasta_sync = {out.fasta_sync}", file=L)
        print(f"  tree_sync  = {out.tree_sync}", file=L)

except Exception as e:
    with open(log, "a") as L:
        print("[sync] RUNTIME ERROR:", e, file=L)
        traceback.print_exc(file=L)
    raise
