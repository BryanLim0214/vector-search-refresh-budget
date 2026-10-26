// Render the icons used in the slides to inline SVG strings for the local preview.
const React = require("react");
const ReactDOMServer = require("react-dom/server");
const fa = require("react-icons/fa");
const map = { Clock: "FaRegClock", Search: "FaSearch", PaperPlane: "FaPaperPlane", Warning: "FaExclamationTriangle", Lightning: "FaBolt", Chart: "FaChartBar" };
const out = {};
for (const [k, v] of Object.entries(map)) out[k] = ReactDOMServer.renderToStaticMarkup(React.createElement(fa[v], { size: "100%" }));
require("fs").writeFileSync(process.argv[2], JSON.stringify(out));
console.log(Object.keys(out).join(","));
