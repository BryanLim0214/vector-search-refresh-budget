// Build the PowerPoint version of the 15-slide deck: one full-slide image per slide (rendered from the
// Slides artifact's own slide files) with the speaker script attached as notes, plus a Markdown script.
// Usage: node build_pptx_from_slides.js <slides folder with project/> <folder of rendered PNGs>
const fs = require("fs");
const path = require("path");
const pptxgen = require("pptxgenjs");

const [, , ROOT, PNG] = process.argv;
const OUT_DIR = __dirname;
const deck = JSON.parse(fs.readFileSync(path.join(ROOT, "project", "deck.json"), "utf8"));

const decode = (s) => s.replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&#160;/g, " ");
const strip = (s) => decode(s.replace(/<[^>]+>/g, "")).replace(/\s+/g, " ").trim();

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE";
pres.title = deck.title;
let script = `# Speaker script: ${deck.title}\n\nOne section per slide, in order. Read it aloud as written; times are rough.\n`;

deck.order.forEach((id, i) => {
  const html = fs.readFileSync(path.join(ROOT, "project", "slides", `${id}.html`), "utf8");
  const title = strip((html.match(/<h[12][^>]*>([\s\S]*?)<\/h[12]>/) || [, id])[1]);
  const notes = ((html.match(/<aside>([\s\S]*?)<\/aside>/) || [, ""])[1]).trim();
  const slide = pres.addSlide();
  slide.addImage({ path: path.join(PNG, `${id}.png`), x: 0, y: 0, w: 13.333, h: 7.5, altText: title });
  slide.addNotes(decode(notes));
  const [timing, ...paras] = decode(notes).split(/\n\s*\n/);
  script += `\n## ${i + 1}. ${title}\n\n*${timing.trim()}*\n\n${paras.map((p) => p.trim()).join("\n\n")}\n`;
});

fs.writeFileSync(path.join(OUT_DIR, "speaker_script.md"), script, "utf8");
pres.writeFile({ fileName: path.join(OUT_DIR, "midterm_progress.pptx") }).then((f) => console.log("wrote", f));
