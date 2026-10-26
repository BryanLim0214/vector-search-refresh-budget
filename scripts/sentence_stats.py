"""Readability gate for the paper PDF: abstract word count and long sentences per section.

Usage: python sentence_stats.py paper/main.pdf [max_words]
"""
import re
import subprocess
import sys

sys.stdout.reconfigure(encoding="utf-8")

pdf = sys.argv[1]
limit = int(sys.argv[2]) if len(sys.argv) > 2 else 40
text = subprocess.run(["pdftotext", "-enc", "UTF-8", "-nopgbrk", pdf, "-"], capture_output=True, text=True,
                      encoding="utf-8").stdout
text = re.sub(r"-\n(?=[a-z])", "", text)  # rejoin hyphenated line breaks
flat = re.sub(r"\s+", " ", text)

# Section boundaries by the IEEE roman-numeral headings.
heads = list(re.finditer(r"\b(Abstract|Index Terms|I\. I ?NTRODUCTION|[IVX]+\. [A-Z][A-Z ]{3,}|R EFERENCES|REFERENCES)", flat))
spans = []
for i, h in enumerate(heads):
    end = heads[i + 1].start() if i + 1 < len(heads) else len(flat)
    spans.append((h.group(1), flat[h.end():end]))

for name, body in spans:
    if name.startswith("R") and "EFERENCES" in name:
        break
    if name == "Index Terms":
        continue
    sents = [s.strip() for s in re.split(r"(?<=[.?!])\s+(?=[A-Z(])", body) if s.strip()]
    lens = [len(s.split()) for s in sents]
    long_ = [(n, s) for n, s in zip(lens, sents) if n > limit]
    words = len(body.split())
    print(f"{name[:28]:28s} words={words:5d} sentences={len(sents):4d} "
          f"max={max(lens) if lens else 0:3d} over{limit}={len(long_)}")
    for n, s in sorted(long_, reverse=True)[:3]:
        print(f"    {n:3d}: {s[:160]}...")
