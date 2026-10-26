"""Record the deck's animations as looping videos and render every slide for the animated PowerPoint.

Usage: python record_animations.py <slides folder with project/> <output folder> <chrome.exe> <ffmpeg.exe>

Each slide page is built the same way as tools/make_preview.py (x-embed becomes an iframe). A small
shim replaces requestAnimationFrame in every frame, so each video frame is drawn at an exact time and
no frame is dropped. Writes, under the output folder:
  bg/<id>.png        full-slide picture (animations at their still frame; build-in charts hidden)
  overlay/<id>_N.png build-in chart with a transparent background, for a PowerPoint entrance effect
  video/<name>.mp4   looping H.264 video of one animated region, and poster/<name>.png (its first frame)
  manifest.json      positions in slide pixels (1920 x 1080), titles and speaker notes
"""
import html, json, re, subprocess, sys
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT, OUT, CHROME, FFMPEG = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], sys.argv[4]
REUSE = len(sys.argv) > 5 and sys.argv[5] == "reuse"   # keep videos already recorded; redo only the stills
FPS = 30
for d in ("bg", "overlay", "video", "poster", "pages"):
    (OUT / d).mkdir(parents=True, exist_ok=True)
deck = json.loads((ROOT / "project" / "deck.json").read_text(encoding="utf-8"))
fonts = "".join(f'<link rel="stylesheet" href="{f["href"]}">' for f in deck["faces"].values() if "href" in f)

# One loop of each animation. start_ms is where the loop begins (the slide's still frame when there is one),
# and offsets give each iframe its own clock, in document order. Regions are in slide pixels.
LOOPS = {
    "cover": {"seconds": 60.0, "offsets": [0], "regions": [("cover", 0, 0, 1920, 1080)]},
    "problem": {"seconds": 12.0, "offsets": [4200], "regions": [("problem", 128, 330, 820, 560)]},
    "question": {"seconds": 6.0, "offsets": [2600, 4000, 4500],
                 "regions": [("question_refresh", 128, 396, 528, 450), ("question_extra", 696, 396, 528, 450),
                             ("question_push", 1264, 396, 528, 450)]},
    "data": {"seconds": 70.4, "offsets": [2100], "regions": [("data", 128, 330, 900, 330)]},
}

# The cover's cloud mixes speeds that never repeat. For a seamless 60-second loop each speed is moved to
# the nearest whole number of turns per minute (rotation 0.1 -> 0.1047, tilt 0.17 -> 0.2094,
# question path 0.29/0.37/0.23 -> 0.3142/0.4189/0.2094). The new-data cycle (12 s) already fits.
COVER_SPEEDS = [
    ("var a=t*0.1,b=0.35+Math.sin(t*0.17)*0.07",
     "var a=t*(2*Math.PI/60),b=0.35+Math.sin(t*(4*Math.PI/60))*0.07"),
    ("var q={x:Math.sin(t*0.29)*0.7,y:Math.sin(t*0.37)*0.5,z:Math.cos(t*0.23)*0.7}",
     "var q={x:Math.sin(t*(6*Math.PI/60))*0.7,y:Math.sin(t*(8*Math.PI/60))*0.5,z:Math.cos(t*(4*Math.PI/60))*0.7}"),
]

SHIM = """(() => {
  let cbs = [];
  window.__vt = 0;
  window.requestAnimationFrame = (cb) => { cbs.push(cb); return cbs.length; };
  window.cancelAnimationFrame = () => {};
  performance.now = () => window.__vt;
  window.__step = (ms) => { window.__vt = ms; const c = cbs; cbs = []; c.forEach((f) => f(ms)); };
})();"""


def page_html(sid, record):
    sec = (ROOT / "project" / "slides" / f"{sid}.html").read_text(encoding="utf-8")
    notes = re.search(r"<aside>(.*?)</aside>", sec, re.S).group(1).strip()
    if record and sid == "cover":
        for old, new in COVER_SPEEDS:
            assert old in sec, old
            sec = sec.replace(old, new)

    def emb(m):
        style = re.search(r'style="([^"]*)"', m.group(1)).group(1)
        return f'<iframe style="{style};border:0" srcdoc="{html.escape(m.group(2), quote=True)}"></iframe>'

    sec = re.sub(r"<x-embed([^>]*)>(.*?)</x-embed>", emb, sec, flags=re.S)
    sec = re.sub(r"<aside>.*?</aside>", "", sec, flags=re.S)
    page = ("<!doctype html><html><head><meta charset='utf-8'>" + fonts +
            "<style>html,body{margin:0;width:1920px;height:1080px;overflow:hidden}"
            "section{position:relative;width:1920px;height:1080px;box-sizing:border-box;overflow:visible}"
            "h1,h2,h3,p,ul,ol{margin:0} *{box-sizing:border-box}</style></head><body>" + sec + "</body></html>")
    p = OUT / "pages" / f"{sid}{'_rec' if record else ''}.html"
    p.write_text(page, encoding="utf-8")
    return p, notes


def ready(page):
    page.wait_for_load_state("networkidle")
    page.evaluate("document.fonts.ready.then(() => true)")
    page.wait_for_timeout(300)


def encode(out_mp4):
    cmd = [FFMPEG, "-y", "-loglevel", "error", "-f", "image2pipe", "-framerate", str(FPS), "-c:v", "png", "-i", "-",
           "-vf", "scale=out_color_matrix=bt709:out_range=tv,format=yuv420p",
           "-c:v", "libx264", "-preset", "slow", "-crf", "16", "-tune", "animation", "-profile:v", "high",
           "-colorspace", "bt709", "-color_primaries", "bt709", "-color_trc", "bt709", "-color_range", "tv",
           "-movflags", "+faststart", str(out_mp4)]
    return subprocess.Popen(cmd, stdin=subprocess.PIPE)


manifest = {"title": deck["title"], "slides": []}
with sync_playwright() as pw:
    browser = pw.chromium.launch(executable_path=CHROME, args=["--hide-scrollbars"])
    still = browser.new_context(viewport={"width": 1920, "height": 1080}, device_scale_factor=1, reduced_motion="reduce")
    moving = browser.new_context(viewport={"width": 1920, "height": 1080}, device_scale_factor=1,
                                 reduced_motion="no-preference")
    moving.add_init_script(SHIM)

    for sid in deck["order"]:
        path, notes = page_html(sid, record=False)
        page = still.new_page()
        page.goto(path.resolve().as_uri())
        ready(page)
        title = page.evaluate("(document.querySelector('h1,h2')||{}).textContent||''").strip()
        entry = {"id": sid, "title": title, "notes": notes, "videos": [], "overlays": []}
        builds = page.query_selector_all("[data-build-in]")
        if builds:
            # Chart pictures on their own, with a transparent background, then the slide without them.
            saved = page.evaluate("[document.querySelector('section').style.background, document.body.style.background]")
            page.evaluate("document.querySelector('section').style.background='transparent';"
                          "document.body.style.background='transparent'")
            for n, el in enumerate(builds):
                box = el.bounding_box()
                others = [b for b in builds if b != el]
                for o in others:
                    o.evaluate("e => e.style.visibility='hidden'")
                png = OUT / "overlay" / f"{sid}_{n}.png"
                page.screenshot(path=str(png), clip=box, omit_background=True)
                for o in others:
                    o.evaluate("e => e.style.visibility=''")
                entry["overlays"].append({"png": str(png.resolve()), "x": box["x"], "y": box["y"],
                                          "w": box["width"], "h": box["height"],
                                          "effect": el.get_attribute("data-build-in")})
            page.evaluate("([s, b]) => { document.querySelector('section').style.background = s;"
                          " document.body.style.background = b; }", saved)
            for el in builds:
                el.evaluate("e => e.style.visibility='hidden'")
        page.screenshot(path=str(OUT / "bg" / f"{sid}.png"))
        entry["bg"] = str((OUT / "bg" / f"{sid}.png").resolve())
        page.close()

        if sid in LOOPS and REUSE and all((OUT / "video" / f"{r[0]}.mp4").exists() for r in LOOPS[sid]["regions"]):
            for name, x, y, w, h in LOOPS[sid]["regions"]:
                entry["videos"].append({"mp4": str((OUT / "video" / f"{name}.mp4").resolve()),
                                        "poster": str((OUT / "poster" / f"{name}.png").resolve()),
                                        "x": x, "y": y, "w": w, "h": h})
        elif sid in LOOPS:
            spec = LOOPS[sid]
            rpath, _ = page_html(sid, record=True)
            page = moving.new_page()
            page.goto(rpath.resolve().as_uri())
            ready(page)
            frames = [f.content_frame() for f in page.query_selector_all("iframe")]
            assert len(frames) == len(spec["offsets"]), (sid, len(frames))
            assert all(fr.evaluate("typeof window.__step === 'function'") for fr in frames), sid
            procs = {}
            for name, x, y, w, h in spec["regions"]:
                mp4 = OUT / "video" / f"{name}.mp4"
                procs[name] = encode(mp4)
                entry["videos"].append({"mp4": str(mp4.resolve()), "poster": str((OUT / "poster" / f"{name}.png").resolve()),
                                        "x": x, "y": y, "w": w, "h": h})
            total = round(spec["seconds"] * FPS)
            for k in range(total):
                t_ms = k * 1000.0 / FPS
                for fr, off in zip(frames, spec["offsets"]):
                    fr.evaluate(f"window.__step({off + t_ms})")
                for name, x, y, w, h in spec["regions"]:
                    shot = page.screenshot(clip={"x": x, "y": y, "width": w, "height": h})
                    if k == 0:
                        (OUT / "poster" / f"{name}.png").write_bytes(shot)
                    procs[name].stdin.write(shot)
            for p in procs.values():
                p.stdin.close()
                assert p.wait() == 0
            page.close()
            print(sid, "recorded", total, "frames", flush=True)
        manifest["slides"].append(entry)
        print(sid, "done", flush=True)
    browser.close()

(OUT / "manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
print("manifest written")
