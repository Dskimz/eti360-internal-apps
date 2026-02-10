import { mkdir, readFile, writeFile } from "node:fs/promises";

const src = "node_modules/@eti360/design-system/eti360.css";

const css = await readFile(src, "utf8");

await writeFile("ui_style.css", css, "utf8");

await mkdir("api/app/static", { recursive: true });
const header = `/*\n  Vendored from ${src}\n  Run: npm run sync:css\n*/\n\n`;
await writeFile("api/app/static/eti360.css", header + css, "utf8");

console.log("Synced design system CSS to ui_style.css and api/app/static/eti360.css");

