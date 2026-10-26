# Midterm presentation

A 15-slide talk about the project, written for a general audience.

| File | What it is |
|---|---|
| `midterm_progress.pptx` | The slides for PowerPoint: one full-slide picture per slide, with the speaker script in the notes. |
| `speaker_script.md` | The speaker script, slide by slide, with rough timings. |
| `deck_text.md` | Every slide's visible text, chart descriptions and notes, for checking facts. |
| `slides_png/` | Each slide as a 1920 x 1080 picture. |
| `slides_source/` | The slide sources: one HTML section per slide plus `deck.json`. The live version uses small animations (the 3D cloud of shards, the out-of-date summary, the three ways to spend the budget, and where new passages land); the pictures show one frame of each. |
| `tools/make_preview.py` | Renders the slide sources to pictures with headless Chrome. Run it with reduced motion so each animation shows its chosen frame. `icons.js` makes the icon file it reads. |
| `build_pptx_from_slides.js` | Builds `midterm_progress.pptx` and `speaker_script.md` from the slide sources and the pictures (needs `pptxgenjs`). |

Every number on the slides comes from the paper's generated macros (`paper_plain/generated/numbers.tex`) or from the result tables in `analysis_root/`. The chart bars use `recent_recall_at_10_mean` from `analysis_root/out_addendum2/m2_conditions.csv` and `m3_conditions.csv`, times 100, to one decimal place.
