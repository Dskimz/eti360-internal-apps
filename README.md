# ETI360 Internal Apps

Static directory page for internal tools.

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
