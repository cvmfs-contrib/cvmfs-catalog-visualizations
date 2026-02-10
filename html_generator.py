# -*- coding: utf-8 -*-
"""
HTML generator for CVMFS catalog visualization.

Generates a self-contained HTML file with an interactive canvas-based sunburst chart,
and a multi-repo viewer page that loads compressed data files.
"""

import json
from tree_builder import CatalogNode

# D3.js CDN URL (used for hierarchy/partition layout only, not rendering)
D3_CDN = "https://d3js.org/d3.v7.min.js"

# fzstd CDN URL (pure JS zstandard decompressor, ~8KB)
FZSTD_CDN = "https://cdn.jsdelivr.net/npm/fzstd/umd/index.js"


def _escape_for_format(s: str) -> str:
    """Escape braces in a string so it can be embedded in a str.format() template."""
    return s.replace("{", "{{").replace("}", "}}")


# ---------------------------------------------------------------------------
# Shared CSS — used by both standalone and viewer pages
# ---------------------------------------------------------------------------
SHARED_CSS = """\
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    background: #1a1a2e;
    color: #eee;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
}

header {
    background: #16213e;
    padding: 1rem 2rem;
    border-bottom: 1px solid #0f3460;
}

header h1 {
    font-size: 1.5rem;
    font-weight: 500;
}

header .repo-name {
    color: #e94560;
    font-family: monospace;
}

.container {
    display: flex;
    flex: 1;
    overflow: hidden;
}

.chart-container {
    flex: 1;
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 1rem;
    max-height: calc(100vh - 60px);
    position: relative;
}

#chart {
    max-width: min(80vh, 800px);
    max-height: 80vh;
}

.sidebar {
    width: 350px;
    background: #16213e;
    padding: 1.5rem;
    overflow-y: auto;
    border-left: 1px solid #0f3460;
}

.sidebar h2 {
    font-size: 1rem;
    font-weight: 500;
    margin-bottom: 1rem;
    color: #e94560;
}

.info-panel {
    background: #1a1a2e;
    border-radius: 8px;
    padding: 1rem;
    margin-bottom: 1rem;
}

.info-row {
    display: flex;
    justify-content: space-between;
    padding: 0.5rem 0;
    border-bottom: 1px solid #0f3460;
}

.info-row:last-child {
    border-bottom: none;
}

.info-label {
    color: #888;
    font-size: 0.85rem;
}

.info-value {
    font-family: monospace;
    font-size: 0.9rem;
}

.info-value.hash {
    max-width: 150px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: pointer;
}
.info-value.hash:hover {
    color: #e94560;
}

.legend {
    position: absolute;
    bottom: 1rem;
    right: 1rem;
    background: rgba(22, 33, 62, 0.9);
    padding: 0.75rem 1rem;
    border-radius: 8px;
    border: 1px solid #0f3460;
}

.legend-title {
    font-size: 0.75rem;
    color: #e94560;
    margin-bottom: 0.5rem;
    font-weight: 500;
}

.legend-item {
    display: flex;
    align-items: center;
    margin-bottom: 0.35rem;
    font-size: 0.75rem;
}

.legend-item:last-child {
    margin-bottom: 0;
}

.legend-color {
    width: 14px;
    height: 14px;
    border-radius: 3px;
    margin-right: 0.5rem;
    flex-shrink: 0;
}


.stats {
    font-size: 0.8rem;
    color: #888;
    margin-top: 1rem;
}

.instructions {
    font-size: 0.8rem;
    color: #666;
    margin-top: 1rem;
    line-height: 1.5;
}

.tips {
    font-size: 0.8rem;
    color: #888;
    margin-top: 1rem;
    line-height: 1.5;
}
.tips strong {
    color: #e94560;
}

.catalog-item {
    padding: 0.3rem 0;
    border-bottom: 1px solid #0f3460;
    font-size: 0.8rem;
    display: flex;
    align-items: baseline;
    gap: 0.3rem;
    cursor: pointer;
}
.catalog-item:hover {
    background: #0f3460;
}
.catalog-item:last-child {
    border-bottom: none;
}
.catalog-size {
    color: #e94560;
    flex-shrink: 0;
}
.catalog-path {
    font-family: monospace;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.path-bar {
    background: #16213e;
    padding: 0.75rem 2rem;
    border-bottom: 1px solid #0f3460;
    font-family: monospace;
    font-size: 0.9rem;
    word-break: break-all;
}
.path-bar .label {
    color: #888;
    margin-right: 0.5rem;
}
.path-bar .path {
    color: #e94560;
}

.incomplete-banner {
    display: none;
    background: #2d1f00;
    border-bottom: 1px solid #6b4f1f;
    padding: 0.5rem 2rem;
    font-size: 0.85rem;
    color: #eab308;
}

@media (max-width: 768px) {
    header h1 {
        font-size: 1.1rem;
    }

    .path-bar {
        padding: 0.5rem 1rem;
        font-size: 0.8rem;
    }

    .incomplete-banner {
        padding: 0.5rem 1rem;
        font-size: 0.75rem;
    }

    .container {
        flex-direction: column;
        overflow: auto;
    }

    .chart-container {
        max-height: none;
        padding: 0.5rem;
        aspect-ratio: 1;
        max-width: 100vw;
    }

    .legend {
        font-size: 0.65rem;
        padding: 0.5rem 0.75rem;
        bottom: 0.5rem;
        right: 0.5rem;
    }

    .legend-title {
        font-size: 0.65rem;
    }

    .legend-item {
        font-size: 0.65rem;
    }

    .legend-color {
        width: 10px;
        height: 10px;
    }

    .sidebar {
        width: 100%;
        border-left: none;
        border-top: 1px solid #0f3460;
    }
}
"""

# ---------------------------------------------------------------------------
# Shared JS — visualization logic, wrapped in initVisualization(config)
#
# config = {
#     data,              // tree object (already parsed)
#     repoName,          // string
#     repoUrl,           // string
#     generatedAt,       // string
#     maxCatalogs,       // number
#     catalogsDownloaded,// number
#     updateUrl,         // boolean (false for standalone, true for viewer)
# }
#
# Returns { clickPath(path) } so the caller can restore a path from URL.
# ---------------------------------------------------------------------------
SHARED_JS = """\
function initVisualization(config) {
    // Enrich tree: recompute depth and cumulative_cost (dropped from JSON for size)
    function enrichTree(node, depth, parentCost) {
        node.depth = depth;
        node.cumulative_cost = parentCost + (node.size || 0);
        if (node.children) {
            for (const c of node.children) enrichTree(c, depth + 1, node.cumulative_cost);
        }
    }
    enrichTree(config.data, 0, 0);

    // Set repo name in header
    document.getElementById('repo-name').textContent = config.repoName;

    const chartContainer = document.querySelector('.chart-container');
    const size = Math.min(chartContainer.clientWidth - 32, chartContainer.clientHeight - 32, 800);
    const width = size;
    const height = size;
    const radius = width / 12;

    // Desaturate a hex color by blending with gray
    function desaturate(hex, amount) {
        if (amount === undefined) amount = 0.4;
        const r = parseInt(hex.slice(1, 3), 16);
        const g = parseInt(hex.slice(3, 5), 16);
        const b = parseInt(hex.slice(5, 7), 16);
        const gray = (r + g + b) / 3;
        const nr = Math.round(r + (gray - r) * amount);
        const ng = Math.round(g + (gray - g) * amount);
        const nb = Math.round(b + (gray - b) * amount);
        return '#' + nr.toString(16).padStart(2, '0') + ng.toString(16).padStart(2, '0') + nb.toString(16).padStart(2, '0');
    }

    // Color scale based on size
    function sizeColor(size) {
        const mb = size / (1024 * 1024);
        if (mb < 2) return "#22c55e";
        if (mb < 10) return "#eab308";
        if (mb < 50) return "#f97316";
        return "#ef4444";
    }

    function getColor(d) {
        if (d.data.is_virtual) return "#4a5568";
        let color = sizeColor(d.data.size || 0);
        if (d.data.is_large && !d.children) {
            color = desaturate(color);
        }
        return color;
    }

    // Format bytes
    function formatBytes(bytes) {
        if (bytes === 0) return "0 B";
        const k = 1024;
        const sizes = ["B", "KB", "MB", "GB"];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
    }

    // Create hierarchy - siblings share parent's arc equally
    const root = d3.hierarchy(config.data);
    root.value = 1;
    root.eachBefore(d => {
        if (d.children) {
            const childValue = d.value / d.children.length;
            d.children.forEach(c => c.value = childValue);
        }
    });

    const partition = d3.partition()
        .size([2 * Math.PI, root.height + 1]);

    partition(root);

    root.each(d => {
        d.current = { x0: d.x0, x1: d.x1, y0: d.y0, y1: d.y1 };
    });

    // Canvas setup with HiDPI support
    const canvas = document.getElementById('chart');
    const dpr = window.devicePixelRatio || 1;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    canvas.style.width = width + 'px';
    canvas.style.height = height + 'px';
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);

    // Precompute flat list of descendants (excluding root) for drawing/hit testing
    const descendants = root.descendants().slice(1);

    // Track state
    let currentNode = root;
    let hoveredNode = null;

    function arcVisible(d) {
        return d.y1 <= 6 && d.y0 >= 1 && d.x1 > d.x0;
    }

    function drawArc(cx, cy, x0, x1, innerR, outerR, color, opacity) {
        if (x1 - x0 < 0.001) return;
        const startAngle = x0 - Math.PI / 2;
        const endAngle = x1 - Math.PI / 2;
        ctx.beginPath();
        ctx.arc(cx, cy, outerR, startAngle, endAngle);
        ctx.arc(cx, cy, innerR, endAngle, startAngle, true);
        ctx.closePath();
        ctx.globalAlpha = opacity;
        ctx.fillStyle = color;
        ctx.fill();
    }

    function draw() {
        const cx = width / 2;
        const cy = height / 2;

        ctx.clearRect(0, 0, width, height);

        // Draw arcs
        for (const d of descendants) {
            if (!arcVisible(d.current)) continue;
            const innerR = d.current.y0 * radius;
            const outerR = Math.max(d.current.y0 * radius, d.current.y1 * radius - 1);
            const color = getColor(d);
            const opacity = d === hoveredNode ? 1 : 0.85;
            drawArc(cx, cy, d.current.x0, d.current.x1, innerR, outerR, color, opacity);
        }

        // Draw center circle
        ctx.beginPath();
        ctx.arc(cx, cy, radius, 0, 2 * Math.PI);
        ctx.closePath();
        ctx.globalAlpha = hoveredNode === currentNode ? 1 : 0.9;
        ctx.fillStyle = getColor(currentNode);
        ctx.fill();

        // Draw center text
        ctx.globalAlpha = 1;
        ctx.fillStyle = '#eee';
        ctx.font = '14px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText('Click for top level', cx, cy);
    }

    function hitTest(clientX, clientY) {
        const rect = canvas.getBoundingClientRect();
        const mx = (clientX - rect.left) * (width / rect.width) - width / 2;
        const my = (clientY - rect.top) * (height / rect.height) - height / 2;
        const r = Math.sqrt(mx * mx + my * my);

        // Check center circle
        if (r < radius) return currentNode;

        let angle = Math.atan2(my, mx) + Math.PI / 2;
        if (angle < 0) angle += 2 * Math.PI;

        for (const d of descendants) {
            if (!arcVisible(d.current)) continue;
            const innerR = d.current.y0 * radius;
            const outerR = Math.max(d.current.y0 * radius, d.current.y1 * radius - 1);
            if (r >= innerR && r <= outerR && angle >= d.current.x0 && angle < d.current.x1) {
                return d;
            }
        }
        return null;
    }

    // Update info panel
    function updateInfo(d) {
        document.getElementById("info-path").textContent = d.data.path || "/";
        document.getElementById("info-size").textContent = formatBytes(d.data.size || 0);
        document.getElementById("info-cost").textContent = formatBytes(d.data.cumulative_cost || 0);
        document.getElementById("info-depth").textContent = d.data.depth || 0;
        document.getElementById("info-hash").textContent = d.data.hash || "-";

        const detailLink = document.getElementById("detail-link");
        if (detailLink && d.data.hash && !d.data.is_virtual && config.repoUrl) {
            detailLink.href = "catalog_detail.html?repo=" + encodeURIComponent(config.repoUrl) +
                "&hash=" + encodeURIComponent(d.data.hash);
            detailLink.style.display = "block";
        } else if (detailLink) {
            detailLink.style.display = "none";
        }
    }

    canvas.addEventListener('mousemove', function(event) {
        const hit = hitTest(event.clientX, event.clientY);
        if (hit !== hoveredNode) {
            hoveredNode = hit;
            canvas.style.cursor = hit ? 'pointer' : 'default';
            if (hit) {
                updateInfo(hit);
            } else {
                updateInfo(currentNode);
            }
            draw();
        }
    });

    canvas.addEventListener('mouseleave', function() {
        if (hoveredNode) {
            hoveredNode = null;
            canvas.style.cursor = 'default';
            updateInfo(currentNode);
            draw();
        }
    });

    canvas.addEventListener('click', function(event) {
        const hit = hitTest(event.clientX, event.clientY);
        if (!hit) return;

        if (hit === currentNode) {
            // Clicking center: jump back to root
            clicked(root);
        } else {
            clicked(hit);
        }
    });

    function navigateTo(p) {
        root.each(d => {
            d.current = {
                x0: Math.max(0, Math.min(1, (d.x0 - p.x0) / (p.x1 - p.x0))) * 2 * Math.PI,
                x1: Math.max(0, Math.min(1, (d.x1 - p.x0) / (p.x1 - p.x0))) * 2 * Math.PI,
                y0: Math.max(0, d.y0 - p.depth),
                y1: Math.max(0, d.y1 - p.depth)
            };
        });

        currentNode = p;
        hoveredNode = null;
        updateInfo(p);
        updateLargestCatalogs(p);
        draw();
    }

    function clicked(p) {
        navigateTo(p);

        if (config.updateUrl !== false) {
            const params = new URLSearchParams(location.search);
            p === root ? params.delete('path') : params.set('path', p.data.path);
            history.pushState(null, '', '?' + params);
        }
    }

    // Update largest catalogs list for a given hierarchy node
    function updateLargestCatalogs(hierarchyNode) {
        const catalogs = hierarchyNode.descendants()
            .filter(d => !d.data.is_virtual && d.data.size > 0)
            .map(d => ({ path: d.data.path, size: d.data.size }))
            .sort((a, b) => b.size - a.size)
            .slice(0, 10);

        const prefix = hierarchyNode.data.path || '/';
        const listHtml = catalogs.map(c => {
            let displayPath = c.path;
            if (prefix !== '/' && displayPath.startsWith(prefix + '/')) {
                displayPath = displayPath.slice(prefix.length);
            }
            return '<div class="catalog-item" data-path="' + c.path + '" title="' + c.path + '">' +
                '<span class="catalog-size" style="color: ' + sizeColor(c.size) + '">' + formatBytes(c.size) + ':</span>' +
                '<span class="catalog-path">' + displayPath + '</span>' +
            '</div>';
        }).join('');
        document.getElementById('largest-catalogs').innerHTML = listHtml;

        // Click to zoom in chart
        document.querySelectorAll('.catalog-item').forEach(item => {
            item.addEventListener('click', () => {
                const targetPath = item.dataset.path;
                const targetNode = root.descendants().find(d => d.data.path === targetPath);
                if (targetNode) {
                    clicked(targetNode);
                }
            });
        });
    }

    // Initial info and sidebar population
    updateInfo(root);
    updateLargestCatalogs(root);

    // Click to copy hash
    document.getElementById('info-hash').addEventListener('click', function() {
        const hash = this.textContent;
        if (hash && hash !== '-') {
            navigator.clipboard.writeText(hash).then(() => {
                const original = this.textContent;
                this.textContent = 'Copied!';
                setTimeout(() => this.textContent = original, 1000);
            });
        }
    });

    // Show incomplete exploration banner if applicable
    (function() {
        const parts = [];
        const stopped = root.descendants().filter(d => d.data.is_large && !d.children);
        if (stopped.length > 0) {
            const totalSize = stopped.reduce((sum, d) => sum + (d.data.size || 0), 0);
            parts.push('exploration stopped at ' +
                stopped.length + ' large catalog' + (stopped.length > 1 ? 's' : '') +
                ' (' + formatBytes(totalSize) + ' unexplored)');
        }
        const maxCatalogs = config.maxCatalogs;
        const catalogsDownloaded = config.catalogsDownloaded;
        if (maxCatalogs > 0 && catalogsDownloaded >= maxCatalogs) {
            parts.push('download limit reached (' + catalogsDownloaded + '/' + maxCatalogs + ' catalogs)');
        }
        if (parts.length > 0) {
            const banner = document.getElementById('incomplete-banner');
            banner.textContent = 'Incomplete: ' + parts.join('; ');
            banner.style.display = 'block';
        }
    })();

    // Initial draw
    draw();

    // Return API for external control (e.g. viewer restoring path from URL)
    return {
        clickPath: function(path) {
            const targetNode = root.descendants().find(d => d.data.path === path);
            if (targetNode) {
                navigateTo(targetNode);
            }
        }
    };
}
"""

# ---------------------------------------------------------------------------
# Shared HTML body — the visualization container (header, chart, sidebar)
# Uses {repo_name}, {generated_at} as format placeholders in standalone mode,
# and literal values in viewer mode.
# ---------------------------------------------------------------------------
_VIZ_BODY_TEMPLATE = """\
    <header>
        <h1><a href="https://chrisburr.github.io/cvmfs-catalog-visualizations/" style="color: inherit; text-decoration: none;">CVMFS Catalog Visualizer</a> - <span class="repo-name" id="repo-name">{repo_name}</span></h1>
    </header>

    <div class="incomplete-banner" id="incomplete-banner"></div>

    <div class="path-bar">
        <span class="label">Selected:</span>
        <span class="path" id="info-path">/</span>
    </div>

    <div class="container">
        <div class="chart-container">
            <canvas id="chart"></canvas>
            <div class="legend">
                <div class="legend-title">Size Legend</div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #22c55e;"></div>
                    <span>&lt; 2 MB</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #eab308;"></div>
                    <span>2 - 10 MB</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #f97316;"></div>
                    <span>10 - 50 MB</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #ef4444;"></div>
                    <span>&gt; 50 MB</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #c15b5b;"></div>
                    <span>Stopped</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #4a5568;"></div>
                    <span>Virtual</span>
                </div>
            </div>
        </div>

        <div class="sidebar">
            <h2>Selected Catalog</h2>
            <div class="info-panel" id="info-panel">
                <div class="info-row">
                    <span class="info-label">Catalog Size</span>
                    <span class="info-value" id="info-size">-</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Cumulative Cost</span>
                    <span class="info-value" id="info-cost">-</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Depth</span>
                    <span class="info-value" id="info-depth">-</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Hash</span>
                    <span class="info-value hash" id="info-hash" title="Click to copy">-</span>
                </div>
                <a id="detail-link" href="#" target="_blank" rel="noopener"
                   style="display: none; margin-top: 0.75rem; padding: 0.5rem 1rem; background: #0f3460; border: 1px solid #4da6ff; border-radius: 4px; color: #4da6ff; font-size: 0.8rem; text-decoration: none; text-align: center;">View directory detail</a>
            </div>

            <h2>Largest Catalogs</h2>
            <div class="info-panel" id="largest-catalogs">
                <!-- Populated by JavaScript -->
            </div>

            <div class="instructions">
                <strong>Instructions:</strong><br>
                • Click on a segment to zoom in<br>
                • Click center to return to top level<br>
                • Hover for details
            </div>

            <div class="tips">
                <strong>Why are catalogs large?</strong><br>
                Catalog size = metadata entries, not file sizes.
                A catalog with many files/directories has a large database.
            </div>

            <div class="stats">
                Generated: {generated_at}
            </div>
        </div>
    </div>
"""

# ---------------------------------------------------------------------------
# Standalone HTML template — self-contained, backwards compatible
# ---------------------------------------------------------------------------
_STANDALONE_TEMPLATE = (
    '<!DOCTYPE html>\n'
    '<html lang="en">\n'
    '<head>\n'
    '    <meta charset="UTF-8">\n'
    '    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
    '    <title>CVMFS Catalog Visualizer - {repo_name}</title>\n'
    '    <script src="{d3_cdn}"></script>\n'
    '    <style>\n'
    + _escape_for_format(SHARED_CSS)
    + '    </style>\n'
    '</head>\n'
    '<body>\n'
    + _escape_for_format(_VIZ_BODY_TEMPLATE)
    + '\n'
    '    <script>\n'
    + _escape_for_format(SHARED_JS)
    + '\n'
    '    var CVMFS_CONFIG = {{\n'
    '        data: {data_json},\n'
    '        repoName: "{repo_name}",\n'
    '        repoUrl: "{repo_url}",\n'
    '        generatedAt: "{generated_at}",\n'
    '        maxCatalogs: {max_catalogs},\n'
    '        catalogsDownloaded: {catalogs_downloaded},\n'
    '        updateUrl: false\n'
    '    }};\n'
    '    initVisualization(CVMFS_CONFIG);\n'
    '    </script>\n'
    '</body>\n'
    '</html>\n'
)


def generate_html(
    root_node: CatalogNode,
    repo_name: str,
    repo_url: str = "",
    generated_at: str = "",
    max_catalogs: int = 0,
    catalogs_downloaded: int = 0,
) -> str:
    """Generate a self-contained HTML visualization.

    Args:
        root_node: Root CatalogNode from tree builder
        repo_name: Repository name for display
        repo_url: Full repository URL for commands
        generated_at: Timestamp string for when the visualization was generated
        max_catalogs: The max_catalogs limit used during the run (0 = unlimited)
        catalogs_downloaded: Number of catalogs actually downloaded

    Returns:
        Complete HTML string
    """
    data_dict = root_node.to_dict()
    data_json = json.dumps(data_dict, separators=(",", ":"))

    return _STANDALONE_TEMPLATE.format(
        repo_name=repo_name,
        repo_url=repo_url or repo_name,
        d3_cdn=D3_CDN,
        data_json=data_json,
        generated_at=generated_at,
        max_catalogs=max_catalogs,
        catalogs_downloaded=catalogs_downloaded,
    )


def generate_data_envelope(
    root_node: CatalogNode,
    repo_name: str,
    repo_url: str = "",
    generated_at: str = "",
    max_catalogs: int = 0,
    catalogs_downloaded: int = 0,
) -> dict:
    """Generate a data envelope dict for external data files.

    Args:
        root_node: Root CatalogNode from tree builder
        repo_name: Repository name for display
        repo_url: Full repository URL for commands
        generated_at: Timestamp string for when the visualization was generated
        max_catalogs: The max_catalogs limit used during the run (0 = unlimited)
        catalogs_downloaded: Number of catalogs actually downloaded

    Returns:
        Dictionary with metadata and tree data
    """
    return {
        "repo_name": repo_name,
        "repo_url": repo_url or repo_name,
        "generated_at": generated_at,
        "max_catalogs": max_catalogs,
        "catalogs_downloaded": catalogs_downloaded,
        "tree": root_node.to_dict(),
    }


# ---------------------------------------------------------------------------
# Viewer-specific CSS
# ---------------------------------------------------------------------------
_VIEWER_CSS = """\
.loading-overlay {
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: #1a1a2e;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    z-index: 1000;
}
.loading-overlay.hidden {
    display: none;
}
.loading-text {
    color: #e94560;
    font-size: 1.2rem;
    margin-bottom: 1.5rem;
}
.progress-bar-container {
    width: 300px;
    height: 6px;
    background: #0f3460;
    border-radius: 3px;
    overflow: hidden;
}
.progress-bar {
    height: 100%;
    background: #e94560;
    border-radius: 3px;
    transition: width 0.15s;
    width: 0%;
}
.progress-bar.indeterminate {
    width: 30%;
    animation: indeterminate 1.5s infinite ease-in-out;
}
@keyframes indeterminate {
    0% { transform: translateX(-100%); }
    100% { transform: translateX(400%); }
}
.progress-size {
    color: #888;
    font-size: 0.8rem;
    margin-top: 0.5rem;
}

.repo-listing {
    max-width: 800px;
    margin: 2rem auto;
    padding: 0 1rem;
}
.repo-listing h1 {
    color: #e94560;
    margin-bottom: 0.5rem;
}
.repo-listing p {
    color: #aaa;
    margin-bottom: 1.5rem;
}
.repo-listing-controls {
    display: flex;
    gap: 0.75rem;
    margin-bottom: 1rem;
    flex-wrap: wrap;
}
.repo-listing-controls input[type="text"] {
    flex: 1;
    min-width: 200px;
    padding: 0.5rem 0.75rem;
    background: #16213e;
    border: 1px solid #0f3460;
    border-radius: 4px;
    color: #eee;
    font-size: 0.9rem;
    outline: none;
}
.repo-listing-controls input[type="text"]:focus {
    border-color: #e94560;
}
.upload-label {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.5rem 0.75rem;
    background: #16213e;
    border: 1px solid #0f3460;
    border-radius: 4px;
    color: #eee;
    font-size: 0.85rem;
    cursor: pointer;
}
.upload-label:hover {
    border-color: #e94560;
    color: #e94560;
}
.upload-label input {
    display: none;
}
.repo-list {
    list-style: none;
    padding: 0;
}
.repo-list li {
    padding: 0.6rem 0;
    border-bottom: 1px solid #222;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.repo-list a {
    color: #4da6ff;
    text-decoration: none;
}
.repo-list a:hover {
    text-decoration: underline;
}
.repo-list .missing {
    color: #666;
}
.repo-list .incomplete {
    color: #eab308;
    font-size: 0.8rem;
}
.repo-list time {
    color: #888;
    font-size: 0.85rem;
    margin-left: auto;
}

#viz-container {
    display: none;
}
#viz-container.visible {
    display: flex;
    flex-direction: column;
    flex: 1;
}

.back-link {
    color: #4da6ff;
    text-decoration: none;
    font-size: 0.85rem;
    margin-left: 1rem;
}
.back-link:hover {
    text-decoration: underline;
}
"""


# ---------------------------------------------------------------------------
# Viewer-specific JS
# ---------------------------------------------------------------------------
_VIEWER_JS = """\
(function() {
    const dataCache = {};
    let currentViz = null;
    let currentRepo = null;

    function timeAgo(date) {
        const seconds = Math.floor((new Date() - date) / 1000);
        if (seconds < 60) return 'just now';
        const intervals = [
            { label: 'year', seconds: 31536000 },
            { label: 'month', seconds: 2592000 },
            { label: 'week', seconds: 604800 },
            { label: 'day', seconds: 86400 },
            { label: 'hour', seconds: 3600 },
            { label: 'minute', seconds: 60 },
        ];
        for (const i of intervals) {
            const count = Math.floor(seconds / i.seconds);
            if (count >= 1) return count + ' ' + i.label + (count > 1 ? 's' : '') + ' ago';
        }
    }

    function formatBytes(bytes) {
        if (bytes === 0) return "0 B";
        const k = 1024;
        const sizes = ["B", "KB", "MB", "GB"];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
    }

    function showLoading(text) {
        document.getElementById('loading-text').textContent = text || 'Loading...';
        document.getElementById('progress-bar').style.width = '0%';
        document.getElementById('progress-bar').classList.remove('indeterminate');
        document.getElementById('progress-size').textContent = '';
        document.getElementById('loading-overlay').classList.remove('hidden');
    }

    function hideLoading() {
        document.getElementById('loading-overlay').classList.add('hidden');
    }

    function updateProgress(loaded, total) {
        const bar = document.getElementById('progress-bar');
        const sizeEl = document.getElementById('progress-size');
        if (total > 0) {
            bar.classList.remove('indeterminate');
            bar.style.width = Math.round((loaded / total) * 100) + '%';
            sizeEl.textContent = formatBytes(loaded) + ' / ' + formatBytes(total);
        } else {
            bar.classList.add('indeterminate');
            sizeEl.textContent = formatBytes(loaded);
        }
    }

    function showListing() {
        document.getElementById('repo-listing').style.display = '';
        document.getElementById('viz-container').classList.remove('visible');
        document.title = 'CVMFS Catalog Visualizations';
        currentRepo = null;
        currentViz = null;
    }

    function showViz() {
        document.getElementById('repo-listing').style.display = 'none';
        document.getElementById('viz-container').classList.add('visible');
    }

    async function fetchWithProgress(url, expectedSize) {
        const response = await fetch(url);
        if (!response.ok) throw new Error('Failed to fetch: ' + response.status);

        const contentLength = response.headers.get('Content-Length');
        const total = contentLength ? parseInt(contentLength, 10) : (expectedSize || 0);

        if (!response.body) {
            // Fallback for browsers without ReadableStream
            const buf = await response.arrayBuffer();
            return new Uint8Array(buf);
        }

        const reader = response.body.getReader();
        const chunks = [];
        let loaded = 0;

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(value);
            loaded += value.length;
            updateProgress(loaded, total);
        }

        const result = new Uint8Array(loaded);
        let offset = 0;
        for (const chunk of chunks) {
            result.set(chunk, offset);
            offset += chunk.length;
        }
        return result;
    }

    async function decompressZstd(compressed) {
        return fzstd.decompress(compressed);
    }

    async function loadRepo(repoName, expectedSize) {
        if (dataCache[repoName]) return dataCache[repoName];

        showLoading('Loading ' + repoName + '...');
        const url = 'data/' + repoName + '.json.zst';
        const compressed = await fetchWithProgress(url, expectedSize);

        showLoading('Decompressing...');
        const decompressed = await decompressZstd(compressed);

        const text = new TextDecoder().decode(decompressed);
        const envelope = JSON.parse(text);
        dataCache[repoName] = envelope;
        return envelope;
    }

    function renderViz(envelope, restorePath) {
        // Reset viz container HTML
        const vizContainer = document.getElementById('viz-container');
        // Clear any previous canvas state
        const oldCanvas = document.getElementById('chart');
        if (oldCanvas) {
            const newCanvas = document.createElement('canvas');
            newCanvas.id = 'chart';
            oldCanvas.parentNode.replaceChild(newCanvas, oldCanvas);
        }

        showViz();
        currentRepo = envelope.repo_name;
        document.title = 'CVMFS Catalog Visualizer - ' + envelope.repo_name;

        // Reset info panel
        document.getElementById("info-path").textContent = "/";
        document.getElementById("info-size").textContent = "-";
        document.getElementById("info-cost").textContent = "-";
        document.getElementById("info-depth").textContent = "-";
        document.getElementById("info-hash").textContent = "-";

        document.getElementById("incomplete-banner").style.display = "none";
        document.getElementById("incomplete-banner").textContent = "";

        // Update generated timestamp
        const statsEl = document.querySelector('.stats');
        if (statsEl && envelope.generated_at) {
            const d = new Date(envelope.generated_at.replace(' ', 'T').replace(' UTC', 'Z'));
            const relative = isNaN(d) ? envelope.generated_at : timeAgo(d);
            statsEl.innerHTML = 'Generated: <span title="' + envelope.generated_at + '" style="cursor: help; border-bottom: 1px dotted #666;">' + relative + '</span>';
        }

        requestAnimationFrame(function() {
            currentViz = initVisualization({
                data: envelope.tree,
                repoName: envelope.repo_name,
                repoUrl: envelope.repo_url,
                generatedAt: envelope.generated_at,
                maxCatalogs: envelope.max_catalogs,
                catalogsDownloaded: envelope.catalogs_downloaded,
                updateUrl: true,
            });

            if (restorePath && restorePath !== '/') {
                currentViz.clickPath(restorePath);
            }
        });
    }

    async function navigateToRepo(repoName, restorePath, expectedSize) {
        try {
            const envelope = await loadRepo(repoName, expectedSize);
            hideLoading();
            renderViz(envelope, restorePath);
        } catch (err) {
            hideLoading();
            alert('Failed to load ' + repoName + ': ' + err.message);
            showListing();
        }
    }

    async function loadRepoListing() {
        try {
            const resp = await fetch('repos.json');
            if (!resp.ok) return;
            const repos = await resp.json();
            renderRepoList(repos);
        } catch (e) {
            // repos.json not available — show empty listing
        }
    }

    function renderRepoList(repos) {
        const list = document.getElementById('repo-list');
        const searchInput = document.getElementById('repo-search');
        let allItems = repos;

        function render(filter) {
            const filtered = filter
                ? allItems.filter(r => r.name.toLowerCase().includes(filter.toLowerCase()))
                : allItems;
            list.innerHTML = filtered.map(r => {
                const badge = r.incomplete ? ' <span class="incomplete">(incomplete)</span>' : '';
                const ts = r.generated_at
                    ? '<time datetime="' + r.generated_at + '" title="' + r.generated_at + '">' + timeAgo(new Date(r.generated_at)) + '</time>'
                    : '';
                return '<li><a href="?repo=' + encodeURIComponent(r.name) + '" data-repo="' + r.name + '" data-size="' + (r.size_bytes || 0) + '">' + r.name + '</a>' + badge + ts + '</li>';
            }).join('');

            // Attach click handlers
            list.querySelectorAll('a[data-repo]').forEach(a => {
                a.addEventListener('click', function(e) {
                    e.preventDefault();
                    const repo = this.dataset.repo;
                    const size = parseInt(this.dataset.size, 10) || 0;
                    history.pushState({ repo: repo }, '', '?repo=' + encodeURIComponent(repo));
                    navigateToRepo(repo, null, size);
                });
            });
        }

        render('');
        searchInput.addEventListener('input', function() {
            render(this.value);
        });
    }

    function handleFileUpload(file) {
        const reader = new FileReader();
        reader.onload = function() {
            try {
                let data;
                if (file.name.endsWith('.zst')) {
                    const compressed = new Uint8Array(reader.result);
                    const decompressed = fzstd.decompress(compressed);
                    data = JSON.parse(new TextDecoder().decode(decompressed));
                } else {
                    data = JSON.parse(new TextDecoder().decode(new Uint8Array(reader.result)));
                }

                // Validate envelope structure
                if (!data.tree || !data.repo_name) {
                    alert('Invalid data file: missing tree or repo_name');
                    return;
                }

                const name = '(local) ' + data.repo_name;
                dataCache[name] = data;
                history.pushState({ repo: name, local: true }, '', '?repo=' + encodeURIComponent(name));
                showViz();
                currentRepo = name;
                document.title = 'CVMFS Catalog Visualizer - ' + data.repo_name;
                requestAnimationFrame(function() {
                    currentViz = initVisualization({
                        data: data.tree,
                        repoName: data.repo_name,
                        repoUrl: data.repo_url,
                        generatedAt: data.generated_at,
                        maxCatalogs: data.max_catalogs,
                        catalogsDownloaded: data.catalogs_downloaded,
                        updateUrl: true,
                    });
                });
            } catch (err) {
                alert('Failed to load file: ' + err.message);
            }
        };
        reader.readAsArrayBuffer(file);
    }

    // Route based on URL
    function route() {
        const params = new URLSearchParams(location.search);
        const repo = params.get('repo');
        const path = params.get('path');

        if (repo) {
            if (dataCache[repo]) {
                hideLoading();
                renderViz(dataCache[repo], path);
            } else {
                navigateToRepo(repo, path, 0);
            }
        } else {
            showListing();
            hideLoading();
        }
    }

    // History navigation
    window.addEventListener('popstate', function() {
        const params = new URLSearchParams(location.search);
        const repo = params.get('repo');
        const path = params.get('path');

        // Same repo: just navigate within the sunburst
        if (repo && repo === currentRepo && currentViz) {
            currentViz.clickPath(path || '/');
            return;
        }

        route();
    });

    // File upload handler
    document.getElementById('file-upload').addEventListener('change', function(e) {
        const file = e.target.files[0];
        if (file) handleFileUpload(file);
    });

    // Back link handler
    document.getElementById('back-to-list').addEventListener('click', function(e) {
        e.preventDefault();
        history.pushState({}, '', location.pathname);
        showListing();
    });

    // Initial route
    loadRepoListing();
    route();
})();
"""


def generate_viewer_html() -> str:
    """Generate the multi-repo viewer HTML page.

    This produces a single HTML page that:
    - Shows a repo listing when no ?repo= param is present
    - Loads and renders visualization data from external .json.zst files
    - Supports file upload of local .json or .json.zst files
    - Uses History API for navigation

    Returns:
        Complete HTML string for the viewer page
    """
    # Build by string concatenation — no str.format(), so no {{/}} escaping needed
    parts = []
    parts.append('<!DOCTYPE html>\n')
    parts.append('<html lang="en">\n')
    parts.append('<head>\n')
    parts.append('    <meta charset="UTF-8">\n')
    parts.append('    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n')
    parts.append('    <title>CVMFS Catalog Visualizations</title>\n')
    parts.append('    <script src="')
    parts.append(D3_CDN)
    parts.append('"></script>\n')
    parts.append('    <script src="')
    parts.append(FZSTD_CDN)
    parts.append('"></script>\n')
    parts.append('    <style>\n')
    parts.append(SHARED_CSS)
    parts.append(_VIEWER_CSS)
    parts.append('    </style>\n')
    parts.append('</head>\n')
    parts.append('<body>\n')

    # Loading overlay
    parts.append('    <div class="loading-overlay hidden" id="loading-overlay">\n')
    parts.append('        <div class="loading-text" id="loading-text">Loading...</div>\n')
    parts.append('        <div class="progress-bar-container">\n')
    parts.append('            <div class="progress-bar" id="progress-bar"></div>\n')
    parts.append('        </div>\n')
    parts.append('        <div class="progress-size" id="progress-size"></div>\n')
    parts.append('    </div>\n')

    # Repo listing
    parts.append('    <div id="repo-listing">\n')
    parts.append('        <div class="repo-listing">\n')
    parts.append('            <h1>CVMFS Catalog Visualizations</h1>\n')
    parts.append('            <p>Interactive sunburst charts showing catalog hierarchy and download costs for CERN CVMFS repositories. ')
    parts.append('<a href="https://github.com/chrisburr/cvmfs-catalog-visualizations" style="color: #4da6ff;">Source on GitHub</a></p>\n')
    parts.append('            <div class="repo-listing-controls">\n')
    parts.append('                <input type="text" id="repo-search" placeholder="Filter repositories...">\n')
    parts.append('                <label class="upload-label">Upload file<input type="file" id="file-upload" accept=".json,.zst"></label>\n')
    parts.append('            </div>\n')
    parts.append('            <ul class="repo-list" id="repo-list"></ul>\n')
    parts.append('        </div>\n')
    parts.append('    </div>\n')

    # Visualization container (hidden until data loads)
    parts.append('    <div id="viz-container">\n')

    # Use the body template with empty placeholders (JS will fill them)
    viz_body = _VIZ_BODY_TEMPLATE.replace('{repo_name}', '').replace('{generated_at}', '')
    # Add back link to header
    viz_body = viz_body.replace(
        '</h1>\n    </header>',
        '</h1>\n        <a href="?" class="back-link" id="back-to-list">All repositories</a>\n    </header>'
    )
    parts.append(viz_body)

    parts.append('    </div>\n')

    # Scripts
    parts.append('    <script>\n')
    parts.append(SHARED_JS)
    parts.append('    </script>\n')
    parts.append('    <script>\n')
    parts.append(_VIEWER_JS)
    parts.append('    </script>\n')
    parts.append('</body>\n')
    parts.append('</html>\n')

    return ''.join(parts)
