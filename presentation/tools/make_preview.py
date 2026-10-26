"""Make a local preview page per slide (approximate render of the Slides format) and screenshot it.

x-embed becomes an iframe (srcdoc), x-icon a small square, aside is hidden. Used only to check
layout: overlaps, text running off its box or off the slide.
"""
import html, json, re, subprocess, sys
from pathlib import Path

ROOT = Path(sys.argv[1])          # folder holding project/
OUT = Path(sys.argv[2])           # output folder for pages and screenshots
BROWSER = sys.argv[3]
OUT.mkdir(parents=True, exist_ok=True)
ICONS = json.loads((Path(__file__).parent / "icons.json").read_text(encoding="utf-8"))
deck = json.loads((ROOT / "project" / "deck.json").read_text(encoding="utf-8"))
fonts = "".join(f'<link rel="stylesheet" href="{f["href"]}">' for f in deck["faces"].values() if "href" in f)

def convert(sec):
    def emb(m):
        attrs, inner = m.group(1), m.group(2)
        style = re.search(r'style="([^"]*)"', attrs).group(1)
        return f'<iframe style="{style};border:0" srcdoc="{html.escape(inner, quote=True)}"></iframe>'
    sec = re.sub(r"<x-embed([^>]*)>(.*?)</x-embed>", emb, sec, flags=re.S)
    sec = re.sub(r'<x-icon name="([^"]*)" style="([^"]*)"></x-icon>',
                 lambda m: f'<div title="{m.group(1)}" style="{m.group(2)};flex:none">{ICONS.get(m.group(1), "")}</div>', sec)
    sec = re.sub(r"<aside>.*?</aside>", "", sec, flags=re.S)
    sec = re.sub(r"<svg ", '<svg style="position:absolute" ', sec, count=0) if False else sec
    return sec

for sid in deck["order"]:
    sec = (ROOT / "project" / "slides" / f"{sid}.html").read_text(encoding="utf-8")
    sec = convert(sec)
    sec = sec.replace("<section ", '<section data-x="1" ', 1)
    page = ("<!doctype html><html><head><meta charset='utf-8'>" + fonts +
            "<style>html,body{margin:0;width:1920px;height:1080px;overflow:hidden}"
            "section{position:relative;width:1920px;height:1080px;box-sizing:border-box;overflow:visible}"
            "h1,h2,h3,p,ul,ol{margin:0} *{box-sizing:border-box}"
            "</style></head><body>" + sec + "</body></html>")
    p = OUT / f"{sid}.html"
    p.write_text(page, encoding="utf-8")
    png = OUT / f"{sid}.png"
    subprocess.run([BROWSER, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--window-size=1920,1080",
                    "--force-prefers-reduced-motion",
                    "--virtual-time-budget=4000", f"--screenshot={png}", p.resolve().as_uri()],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)
    print(sid, png.exists())
