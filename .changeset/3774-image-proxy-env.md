---
"emdash": patch
---

Fixes local media image optimization when EmDash runs behind an HTTPS-terminating reverse proxy without a hard-coded `siteUrl`. The Astro integration now reads `EMDASH_SITE_URL` / `SITE_URL` at build time to register `image.remotePatterns` for the public origin, so local media is served through the `/_image` endpoint instead of passed through as raw originals.
