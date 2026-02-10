# ETI360 Internal Apps

Static directory page for internal tools.

UI styling is aligned with the ETI360 marketing site via `@eti360/design-system`.

## Password-protected deploy (Render)

This repo can be deployed as a password-protected static site by building an encrypted `dist/index.html` via PageCrypt.

### Render settings

- **Type:** Static Site
- **Branch:** `main`
- **Build Command:** `npm install && npm run build`
- **Publish Directory:** `dist`
- **Environment:** add an env var named `PASSWORD` (the shared password users will enter)

### Notes

- The build inlines `ui_style.css` + `apps.json` into the HTML before encrypting, so those files are not exposed when publishing `dist`.
- If `@eti360/design-system` is installed, the build will inline `node_modules/@eti360/design-system/eti360.css` instead of `ui_style.css`.
- To sync local copies (for the Python API static CSS + fallback directory CSS): `npm run sync:css`
