import { mkdir, readFile, writeFile } from "node:fs/promises";

// pagecrypt has a Node ESM bug where it references `window` even when running on the server.
// Shim a minimal `window` so the import works reliably in Render's Node build environment.
if (typeof globalThis.window === "undefined") {
  // Node 20+ has globalThis.crypto; fall back to crypto.webcrypto.
  const cryptoMod = await import("node:crypto");
  globalThis.window = { crypto: globalThis.crypto || cryptoMod.webcrypto };
}

const { encryptHTML } = await import("pagecrypt");

async function loadCss() {
  try {
    return await readFile("node_modules/@eti360/design-system/eti360.css", "utf8");
  } catch {
    return await readFile("ui_style.css", "utf8");
  }
}

const password = process.env.PASSWORD;
const encrypt = Boolean(password && String(password).trim());

const [html, css, appsJsonRaw] = await Promise.all([
  readFile("index.html", "utf8"),
  loadCss(),
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

if (encrypt) {
  const encrypted = await encryptHTML(merged, String(password).trim(), 3e6);
  await writeFile("dist/index.html", encrypted, "utf8");
} else {
  console.warn("PASSWORD env var not set; writing unencrypted dist/index.html");
  await writeFile("dist/index.html", merged, "utf8");
}
