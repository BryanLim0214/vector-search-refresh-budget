"""Gates for the short plain-English paper in paper_plain/.

Builds the PDF with TeX Live, then checks length, readability, jargon, numbers, citations and double-blind
rules. Prints one line per gate and exits 1 if any hard gate fails.
Usage (repository root): .venv/Scripts/python.exe scripts/plain_gates.py
Settings, all optional:
  DVS_PYTHON       Python used to run paper/scripts/gen_numbers.py (default: the Python running this script).
                   gen_numbers.py needs the raw run folders, which are not in this repository; without them it
                   fails and the committed numbers files stay as they are.
  DVS_TEX_BIN      folder holding latexmk, pdftotext and pdfinfo (default: found on PATH). Put TeX Live first:
                   a latexmk without Perl, as in some MiKTeX setups, fails without a clear message.
  DVS_BLIND_NAMES  extra names for the double-blind check, as a regular expression such as "Name1|Name2".
"""
import hashlib
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
PLAIN = ROOT / "paper_plain"
PYTHON = os.environ.get("DVS_PYTHON", sys.executable)
# sources/refs.bib is the local citation ledger and is not in this repository; paper/refs.bib was checked
# identical to it, so it stands in when the ledger is missing.
LEDGER_BIB = ROOT / "sources" / "refs.bib"
if not LEDGER_BIB.exists():
    LEDGER_BIB = ROOT / "paper" / "refs.bib"
MAX_PAGES = 8
ABSTRACT_MAX_WORDS, ABSTRACT_MAX_SENT, ABSTRACT_MAX_NUMBERS = 180, 25, 6
PROSE_MAX_SENT, NUMBERS_PER_SENT = 30, 2

# Short codes a beginner cannot read. Any of these in rendered prose fails.
CODES = (r"DC|RN|PV|ES|ESO|QL|SS|AF|NR|LK|M1|M2|M3|H1|H2|H2b|H3|H3b|H4|H4b|GT2|GT3|PR2|PR3"
         r"|X1|X2|X3|X4|P1|P2|pp|CIs?|HNSW|ANN|FIFO|Sens")
BANNED = (r"novel|robust|leverage|seamless|crucial|delve|notably|furthermore|moreover|"
          r"state-of-the-art|paradigm|cutting-edge|comprehensive|utilize|facilitate|holistic|"
          r"pivotal|intricate|landscape|realm|underscore|showcase|counterfactual\w*|"
          r"amortiz\w*|orthogonal|nontrivial|leverag\w*")
# Technical words allowed only once defined; counted per section for the reviewers.
WATCH = r"epoch|centroid|k-means|embedding|coordinator|recall@10|recall|Spearman|bootstrap|percentile|interval|budget"

env = dict(os.environ)
if os.environ.get("DVS_TEX_BIN"):
    env["PATH"] = os.environ["DVS_TEX_BIN"] + os.pathsep + env["PATH"]
env.update(OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", MKL_NUM_THREADS="1")


def tool(name):
    """Full path of a TeX tool, looked up on the PATH given to the child (Windows otherwise uses ours)."""
    return shutil.which(name, path=env["PATH"]) or name


fails = []


def gate(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + (f": {detail}" if detail else ""))
    if not ok:
        fails.append(name)


def info(name, detail):
    print("INFO " + name + ": " + detail)


def strip_comments(t):
    return "\n".join(re.sub(r"(?<!\\)%.*$", "", line) for line in t.splitlines())


def prose_of(tex):
    """Body prose of a .tex file: no comments, floats, equations or command names."""
    t = strip_comments(tex)
    t = re.sub(r"\\begin\{(table\*?|figure\*?|tabular|equation\*?|align\*?|tikzpicture)\}.*?\\end\{\1\}", " ", t, flags=re.S)
    t = re.sub(r"\\RNum[A-Za-z]+\{\}", " NUM ", t)
    t = re.sub(r"\\RNum[A-Za-z]+", " NUM ", t)
    t = re.sub(r"\\(cite|ref|label|eqref)\{[^}]*\}", " ", t)
    t = re.sub(r"\\(textbf|textit|emph|section\*?|subsection\*?|paragraph)\{([^}]*)\}", r" \2. ", t)
    t = re.sub(r"\$[^$]*\$", " MATH ", t)
    t = re.sub(r"\\[a-zA-Z]+\*?(\[[^\]]*\])?", " ", t)
    t = t.replace("{", " ").replace("}", " ").replace("~", " ")
    return re.sub(r"\s+", " ", t).strip()


def sentences(text):
    parts = re.split(r"(?<=[.?!])\s+(?=[A-Z(])", text)
    return [p for p in parts if len(p.split()) > 2]


def syllables(word):
    w = re.sub(r"[^a-z]", "", word.lower())
    if not w:
        return 0
    groups = re.findall(r"[aeiouy]+", w)
    n = len(groups) - (1 if w.endswith("e") and len(groups) > 1 and not w.endswith("le") else 0)
    return max(1, n)


def grade_level(text):
    """Flesch-Kincaid grade of plain prose (rough: placeholders NUM and MATH count as one-syllable words)."""
    ss = sentences(text)
    words = [w for s in ss for w in s.split()]
    if not ss or not words:
        return 0.0
    return 0.39 * len(words) / len(ss) + 11.8 * sum(syllables(w) for w in words) / len(words) - 15.59


def count_numbers(s):
    return s.count("NUM") + len(re.findall(r"(?<![\w.])\d[\d,]*(\.\d+)?%?", s))


# 1. Numbers, then build.
gn = subprocess.run([PYTHON, "scripts/gen_numbers.py"], cwd=ROOT / "paper", env=env, capture_output=True)
info("gen_numbers", "ran" if gn.returncode == 0 else "failed (raw run folders missing?); committed numbers kept")
os.chdir(PLAIN)
# main.tex shows the author; main_anonymous.tex is the double-blind copy for review.
for job in ("main.tex", "main_anonymous.tex"):
    subprocess.run([tool("latexmk"), "-pdf", "-interaction=nonstopmode", "-quiet", job], env=env,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
anon_log = PLAIN / "main_anonymous.log"
m_anon = re.search(r"Output written on main_anonymous\.pdf \((\d+) pages", anon_log.read_text(encoding="latin-1")) if anon_log.exists() else None
anon_pages = int(m_anon.group(1)) if m_anon else 0
gate("anonymous copy pages <= %d" % MAX_PAGES, 0 < anon_pages <= MAX_PAGES, str(anon_pages))
pdf, log = PLAIN / "main.pdf", PLAIN / "main.log"
sources = [PLAIN / "main.tex", *(PLAIN / "sections").glob("*.tex"), *(PLAIN / "generated").glob("*.tex"),
           *(PLAIN / "figures").glob("*.pdf"), PLAIN / "refs.bib"]
newest = max(p.stat().st_mtime for p in sources if p.exists())
gate("build fresh", pdf.exists() and pdf.stat().st_mtime >= newest)
logt = log.read_text(encoding="latin-1") if log.exists() else ""
m = re.search(r"Output written on main\.pdf \((\d+) pages", logt)
pages = int(m.group(1)) if m else 0
gate("pages <= %d" % MAX_PAGES, 0 < pages <= MAX_PAGES, str(pages))
gate("latex errors", len(re.findall(r"(?m)^! ", logt)) == 0, str(len(re.findall(r"(?m)^! ", logt))))
gate("overfull boxes", "Overfull \\hbox" not in logt, str(logt.count("Overfull \\hbox")))
gate("undefined refs/cites", "undefined" not in logt.lower(), "")

# 2. Sources.
main_tex = (PLAIN / "main.tex").read_text(encoding="utf-8")
sec_files = sorted((PLAIN / "sections").glob("*.tex"))
secs = {p.stem: p.read_text(encoding="utf-8") for p in sec_files}
all_tex = main_tex + "\n" + "\n".join(secs.values())
ab = re.search(r"\\begin\{abstract\}(.*?)\\end\{abstract\}", main_tex, re.S)
abstract_tex = ab.group(1) if ab else ""
title = re.search(r"\\title\{(.*?)\}\s*\n", main_tex, re.S)
info("title", title.group(1).strip() if title else "(none)")

intro = secs.get("intro", "")
gate("no citations in introduction", "\\cite" not in strip_comments(intro))
gate("no math in abstract", "$" not in strip_comments(abstract_tex))
gate("no citations in abstract", "\\cite" not in abstract_tex)
gate("no em/en dashes", not re.search("\u2014|\u2013|(?<!-)--(?!-)|---", strip_comments(all_tex)))
bw = sorted(set(m.group(0).lower() for m in re.finditer(rf"\b({BANNED})\b", prose_of(all_tex), re.I)))
gate("banned words", not bw, ",".join(bw))

# 3. Rendered text: codes, abstract length, double-blind.
txt = subprocess.run([tool("pdftotext"), "-enc", "UTF-8", "-nopgbrk", str(pdf), "-"], env=env,
                     capture_output=True).stdout.decode("utf-8", "replace") if pdf.exists() else ""
body = txt.split("References")[0] if "References" in txt else txt
body_prose = re.sub(r"TABLE [IVX]+.*?(?=\n\n)", " ", body, flags=re.S)
codes = sorted(set(re.findall(rf"(?<![\w@-])({CODES})(?![\w@-])", body_prose)))
gate("no short codes in rendered text", not codes, ",".join(codes))
rab = re.search(r"Abstract\W+(.*?)(Index Terms|I\. I\s*NTRODUCTION)", txt, re.S)
abw = len(rab.group(1).split()) if rab else 0
gate("abstract words <= %d" % ABSTRACT_MAX_WORDS, 0 < abw <= ABSTRACT_MAX_WORDS, str(abw))
ab_prose = prose_of(abstract_tex)
ab_long = [len(s.split()) for s in sentences(ab_prose) if len(s.split()) > ABSTRACT_MAX_SENT]
gate("abstract sentences <= %d words" % ABSTRACT_MAX_SENT, not ab_long, str(ab_long))
abn = count_numbers(ab_prose)
gate("abstract numbers <= %d" % ABSTRACT_MAX_NUMBERS, abn <= ABSTRACT_MAX_NUMBERS, str(abn))
blind_pat = r"(?i)\b(?!\d+\b)[0-9a-f]{7,40}\b|github|gitlab|https?://(?!doi|arxiv|dx)|\S+@\S+\.\w+|COSC|universit"
if os.environ.get("DVS_BLIND_NAMES"):
    blind_pat += "|" + os.environ["DVS_BLIND_NAMES"]
# The double-blind check reads the anonymous copy and the sources without the named author block.
anon_pdf = PLAIN / "main_anonymous.pdf"
anon_txt = subprocess.run([tool("pdftotext"), "-enc", "UTF-8", "-nopgbrk", str(anon_pdf), "-"], env=env,
                          capture_output=True).stdout.decode("utf-8", "replace") if anon_pdf.exists() else ""
gate("anonymous copy built", bool(anon_txt.strip()))
main_tex_blind = re.sub(r"\\else\s*\n\\author\{.*?\\fi", "", main_tex, flags=re.S)
blind = sorted(set(re.findall(blind_pat, anon_txt.split("References")[0] + main_tex_blind)))
gate("double-blind (anonymous copy)", not blind, ",".join(blind))
meta = subprocess.run([tool("pdfinfo"), str(pdf)], env=env, capture_output=True).stdout.decode("utf-8", "replace") if pdf.exists() else ""
gate("pdf metadata has no author", not re.search(r"(?m)^Author:\s*\S", meta))

# 4. Per-section prose: sentence length, numbers per sentence, inline math, reading grade, watch words.
for name, tex in [("abstract", abstract_tex)] + list(secs.items()):
    p = prose_of(tex)
    ss = sentences(p)
    longs = [s for s in ss if len(s.split()) > PROSE_MAX_SENT]
    heavy = [s for s in ss if count_numbers(s) > NUMBERS_PER_SENT]
    nmath = len(re.findall(r"\$[^$]+\$", re.sub(r"\\begin\{(table\*?|figure\*?)\}.*?\\end\{\1\}", " ", strip_comments(tex), flags=re.S)))
    watch = sorted(set(m.group(0).lower() for m in re.finditer(rf"\b({WATCH})\b", p, re.I)))
    gate(f"{name}: sentences <= {PROSE_MAX_SENT} words", not longs,
         "; ".join(f"{len(s.split())}w: {s[:90]}" for s in longs[:3]))
    gate(f"{name}: <= {NUMBERS_PER_SENT} numbers per sentence", not heavy,
         "; ".join(f"{count_numbers(s)}n: {s[:90]}" for s in heavy[:3]))
    if name in ("abstract", "intro", "conclusion"):
        gate(f"{name}: no inline math", nmath == 0, str(nmath))
    words = len(p.split())
    info(name, f"{words} words, {len(ss)} sentences, grade level {grade_level(p):.1f}, inline math {nmath}, watch words: {','.join(watch)}")

# 5. Hand-typed decimals in prose, citations, refs.
typed = sorted(set(re.findall(r"(?<![\\\w{.])\d+\.\d+", prose_of(all_tex))))
gate("no hand-typed decimals in prose", not typed, ",".join(typed))
keys = set(k.strip() for c in re.findall(r"\\cite\{([^}]*)\}", strip_comments(all_tex)) for k in c.split(","))
bib = LEDGER_BIB.read_text(encoding="utf-8")
missing = sorted(k for k in keys if not re.search(r"@\w+\{" + re.escape(k) + ",", bib))
gate("every cite key in the bibliography", not missing, ",".join(missing))
gate("refs.bib identical to the paper's", (PLAIN / "refs.bib").exists() and
     hashlib.sha256((PLAIN / "refs.bib").read_bytes()).hexdigest() == hashlib.sha256(LEDGER_BIB.read_bytes()).hexdigest())
info("citations used", str(len(keys)))

print("\nRESULT: " + ("ALL PASS" if not fails else f"{len(fails)} FAIL: " + "; ".join(fails)))
sys.exit(1 if fails else 0)
