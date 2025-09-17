Local data lives here and is ignored by git. Keep subfolders organized by source and product:

- raw/: original downloads, never modified
- external/: third-party or large cached assets
- processed/: harmonized, analysis-ready outputs derived from raw

Tip: Use scripts in src/ to read/write consistently so paths are reproducible.
