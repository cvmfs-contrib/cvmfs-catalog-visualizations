# CVMFS Catalog Visualizations

[![Generate CVMFS Catalog Visualizations](https://github.com/chrisburr/cvmfs-catalog-visualizations/actions/workflows/pages.yml/badge.svg)](https://github.com/chrisburr/cvmfs-catalog-visualizations/actions/workflows/pages.yml)
[![Deploy to GitHub Pages](https://github.com/chrisburr/cvmfs-catalog-visualizations/actions/workflows/deploy.yml/badge.svg)](https://github.com/chrisburr/cvmfs-catalog-visualizations/actions/workflows/deploy.yml)

Interactive sunburst charts showing the catalog hierarchy and download costs for CERN CVMFS repositories.

**Live site:** https://chrisburr.github.io/cvmfs-catalog-visualizations/

## How it works

A GitHub Actions workflow runs every 6 hours to:
1. Generate compressed data files for 36+ CVMFS repositories in parallel
2. Deploy a single-page viewer and data files to GitHub Pages

Each repository is processed independently - failures don't affect other repos.

### Catalog hierarchy viewer

The main page shows the **catalog tree** for each repository as a sunburst chart, sized equally per sibling and colored by catalog size (MB). This reveals the structure of nested catalogs and their cumulative download cost.

### Catalog directory detail

The [catalog detail page](https://cvmfs-contrib.github.io/cvmfs-catalog-visualizations/catalog_detail.html) drills into a **single catalog** showing its directory structure as a sunburst, sized by entry count. It fetches and decompresses the CVMFS catalog SQLite database directly in the browser using `DecompressionStream` and [sql.js](https://github.com/sql-js/sql.js), with no server or build step required.

Accessible via the "View directory detail" link in the catalog info panel, or directly with URL parameters:
```
catalog_detail.html?repo=http://...&hash=<40-char-hex>
```

## Manual trigger

Go to Actions > "Generate CVMFS Catalog Visualizations" > "Run workflow"
