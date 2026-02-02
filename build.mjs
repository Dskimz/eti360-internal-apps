import { mkdir, readFile, writeFile } from "node:fs/promises";

import { encryptHTML } from "pagecrypt";

const password = process.env.PASSWORD;
if (!password) {
  console.error("Missing PASSWORD env var (set this in Render Environment).");
  process.exit(1);
}

const [html, css, appsJsonRaw] = await Promise.all([
  readFile("index.html", "utf8"),
  readFile("ui_style.css", "utf8"),
  readFile("apps.json", "utf8"),
]);

const appsJson = appsJsonRaw.replace(/<\/script/gi, "<\\/script");

let merged = html;

merged = merged.replace(
  /<link\s+rel=["']stylesheet["']\s+href=["']ui_style\.css["']\s*\/?>/i,
  `<style>\n${css}\n</style>`,
);

merged = merged.replace("<!-- INLINE_CSS -->", "");

merged = merged.replace(
  "<!-- INLINE_APPS_JSON -->",
  `<script id="apps-json" type="application/json">\n${appsJson}\n</script>`,
);

await mkdir("dist", { recursive: true });

const encrypted = await encryptHTML(merged, password, 3e6);
await writeFile("dist/index.html", encrypted, "utf8");
