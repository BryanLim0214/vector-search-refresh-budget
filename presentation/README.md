# Midterm presentation

A 15-slide talk about the project, written for a general audience.

| File | What it is |
|---|---|
| `midterm_progress.pptx` | The slides for PowerPoint, matching the web slides. Each slide is a full-slide picture; the moving parts (the 3D cloud, the out-of-date summary, the three ways to spend the budget, where new passages land) are looping videos that start on their own; the result charts float in; every slide fades in; the speaker script is in the notes. |
| `record_animations.py` | Records each animation frame by frame from the slide sources (Playwright with Chrome, then ffmpeg) and renders the still pictures. The cover's speeds are rounded to whole turns per minute so its 60-second video loops without a jump. |
| `build_pptx_animated.ps1` | Builds `midterm_progress.pptx` with PowerPoint itself (Windows, no window shown): pictures, looping videos, Float In charts, fade transitions and notes. |
| `check_pptx_animated.ps1` | Opens the deck read-only and prints what PowerPoint stored for each slide (videos play on their own and loop, effects, transitions, notes), and exports each slide as a picture. |
| `speaker_script.md` | The speaker script, slide by slide, with rough timings. |
| `deck_text.md` | Every slide's visible text, chart descriptions and notes, for checking facts. |
| `slides_png/` | Each slide as a 1920 x 1080 picture. |
| `slides_source/` | The slide sources: one HTML section per slide plus `deck.json`. The live version uses small animations (the 3D cloud of shards, the out-of-date summary, the three ways to spend the budget, and where new passages land); the pictures show one frame of each. |
| `tools/make_preview.py` | Renders the slide sources to pictures with headless Chrome. Run it with reduced motion so each animation shows its chosen frame. `icons.js` makes the icon file it reads. |
| `build_pptx_from_slides.js` | Builds `speaker_script.md` and `midterm_progress_pictures_only.pptx`, a PowerPoint without animation, from the slide sources and the pictures (needs `pptxgenjs`). |

Every number on the slides comes from the paper's generated macros (`paper_plain/generated/numbers.tex`) or from the result tables in `analysis_root/`. The chart bars use `recent_recall_at_10_mean` from `analysis_root/out_addendum2/m2_conditions.csv` and `m3_conditions.csv`, times 100, to one decimal place.
