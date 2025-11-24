from __future__ import annotations

import json
from typing import Sequence


def render_lineage_html(
    tables: Sequence[dict[str, object]],
    lineage: Sequence[dict[str, object]],
    *,
    title: str = "SQL Column Lineage",
) -> str:
    """Render a standalone HTML page visualising column lineage."""

    tables_json = json.dumps(list(tables))
    lineage_json = json.dumps(list(lineage))

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{
      color-scheme: dark;
      font-family: "Fira Code", "SFMono-Regular", Consolas, "Liberation Mono", monospace;
      --bg: #0c0d10;
      --panel: #161821;
      --text: #f4f4f6;
      --muted: #8a8f9f;
      --highlight: #f5c542;
      --accent: #ff6f61;
      --border: #2b2f3c;
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      padding: 0;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
    }}
    h1 {{
      font-size: 1.2rem;
      font-weight: 600;
      margin-bottom: 12px;
    }}
    #visualizer {{
      position: relative;
      overflow-x: auto;
      overflow-y: hidden;
    }}
    #tables {{
      display: flex;
      flex-direction: column;
      gap: 18px;
      position: relative;
      z-index: 1;
    }}
    .table {{
      display: flex;
      flex-direction: column;
      gap: 4px;
      padding: 4px 0;
    }}
    .table-name {{
      color: var(--muted);
      letter-spacing: 0.03em;
    }}
    .columns {{
      width: max-content;
      min-width: 100%;
      border-collapse: collapse;
      table-layout: auto;
    }}
    .column {{
      position: relative;
      cursor: pointer;
      padding: 6px 8px;
      min-width: 110px;
      border-radius: 0;
      transition: background 0.2s ease, color 0.2s ease;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .column:hover {{
      background: rgba(245, 197, 66, 0.15);
    }}
    .column.hovered {{
      background: rgba(245, 197, 66, 0.2);
      color: var(--highlight);
    }}
    .column.related {{
      background: rgba(255, 111, 97, 0.15);
      color: var(--accent);
    }}
    #connection-layer {{
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
      pointer-events: none;
    }}
    .lineage-path {{
      fill: none;
      stroke-width: 2px;
      opacity: 0.1;
    }}
    .lineage-path.visible {{
      opacity: 0.7;
    }}
    .lineage-path.direct {{
      stroke: var(--highlight);
    }}
    .lineage-path.indirect {{
      stroke: var(--accent);
      stroke-dasharray: 4 6;
    }}
    .legend {{
      display: flex;
      gap: 16px;
      margin-bottom: 12px;
      font-size: 0.85rem;
      color: var(--muted);
    }}
    .legend-item {{
      display: flex;
      align-items: center;
      gap: 6px;
    }}
    .legend-swatch {{
      width: 14px;
      height: 4px;
      border-radius: 99px;
    }}
    .legend-swatch.direct {{
      background: var(--highlight);
    }}
    .legend-swatch.indirect {{
      background: var(--accent);
    }}
  </style>
</head>
<body>
  <div class="legend">
    <div class="legend-item">
      <span class="legend-swatch direct"></span>Direct dependency
    </div>
    <div class="legend-item">
      <span class="legend-swatch indirect"></span>Transitive dependency
    </div>
  </div>
  <div id="visualizer">
    <svg id="connection-layer"></svg>
    <div id="tables"></div>
  </div>
  <script>
    const tablesData = {tables_json};
    const lineageData = {lineage_json};

    const tablesRoot = document.getElementById("tables");
    const svgLayer = document.getElementById("connection-layer");

    const columnElements = new Map();
    const connections = [];
    const outgoing = new Map();
    const incoming = new Map();

    const columnId = (tableIndex, columnName) => `${{tableIndex}}::${{columnName}}`;

    function buildTables() {{
      tablesData.forEach((table, tableIndex) => {{
        const tableEl = document.createElement("div");
        tableEl.className = "table";

        const nameEl = document.createElement("div");
        nameEl.className = "table-name";
        nameEl.textContent = table.name;
        tableEl.appendChild(nameEl);

        const headerTable = document.createElement("table");
        headerTable.className = "columns preview-table";
        const thead = document.createElement("thead");
        const headerRow = document.createElement("tr");

        table.columns.forEach((columnName) => {{
          const th = document.createElement("th");
          th.className = "column";
          th.textContent = columnName;
          th.tabIndex = 0;

          const id = columnId(tableIndex, columnName);
          th.dataset.columnId = id;
          headerRow.appendChild(th);
          columnElements.set(id, th);
        }});

        thead.appendChild(headerRow);
        headerTable.appendChild(thead);

        const previewBody = document.createElement("tbody");
        previewBody.className = "preview-body";
        headerTable.appendChild(previewBody);

        tableEl.appendChild(headerTable);
        tablesRoot.appendChild(tableEl);
      }});
    }}

    function addDirected(adj, a, b) {{
      if (!adj.has(a)) {{
        adj.set(a, new Set());
      }}
      adj.get(a).add(b);
    }}

    function buildConnections() {{
      lineageData.forEach((edge) => {{
        const fromId = columnId(edge.from[0], edge.from[1]);
        const toId = columnId(edge.to[0], edge.to[1]);
        const fromEl = columnElements.get(fromId);
        const toEl = columnElements.get(toId);
        if (!fromEl || !toEl) {{
          return;
        }}

        addDirected(outgoing, fromId, toId);
        addDirected(incoming, toId, fromId);

        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        path.classList.add("lineage-path");
        svgLayer.appendChild(path);
        connections.push({{ path, fromId, toId }});
      }});

      updateConnectionPaths();
      window.addEventListener("resize", updateConnectionPaths);
    }}

    function updateConnectionPaths() {{
      const bounds = svgLayer.getBoundingClientRect();
      connections.forEach(({{
        path, fromId, toId
      }}) => {{
        const fromEl = columnElements.get(fromId);
        const toEl = columnElements.get(toId);
        if (!fromEl || !toEl) {{
          return;
        }}
        const start = fromEl.getBoundingClientRect();
        const end = toEl.getBoundingClientRect();

        const startX = start.left + start.width / 2 - bounds.left;
        const startY = start.top + start.height / 2 - bounds.top;
        const endX = end.left + end.width / 2 - bounds.left;
        const endY = end.top + end.height / 2 - bounds.top;

        const delta = Math.max(Math.abs(endY - startY) * 0.5, 40);
        const control1X = startX;
        const control1Y = startY + delta;
        const control2X = endX;
        const control2Y = endY - delta;

        const d = `M ${{startX}} ${{startY}} C ${{control1X}} ${{control1Y}}, ${{control2X}} ${{control2Y}}, ${{endX}} ${{endY}}`;
        path.setAttribute("d", d);
      }});
    }}

    function clearHighlights() {{
      columnElements.forEach((el) => {{
        el.classList.remove("hovered", "related");
      }});
      connections.forEach(({{
        path
      }}) => {{
        path.classList.remove("visible", "direct", "indirect");
      }});
    }}

    function collectReachable(startId, graph) {{
      const visited = new Set();
      const initial = graph.get(startId);
      if (!initial) {{
        return visited;
      }}
      const queue = [...initial];
      initial.forEach((neighbor) => visited.add(neighbor));
      while (queue.length) {{
        const current = queue.shift();
        const neighbors = graph.get(current);
        if (!neighbors) {{
          continue;
        }}
        neighbors.forEach((neighbor) => {{
          if (!visited.has(neighbor)) {{
            visited.add(neighbor);
            queue.push(neighbor);
          }}
        }});
      }}
      return visited;
    }}

    function highlightLineage(targetId) {{
      const upstream = collectReachable(targetId, incoming);
      const downstream = collectReachable(targetId, outgoing);
      const related = new Set([...upstream, ...downstream]);

      columnElements.forEach((el, id) => {{
        el.classList.remove("hovered", "related");
        if (id === targetId) {{
          el.classList.add("hovered");
        }} else if (related.has(id)) {{
          el.classList.add("related");
        }}
      }});

      connections.forEach(({{
        path, fromId, toId
      }}) => {{
        path.classList.remove("visible", "direct", "indirect");
        const involvesTarget = fromId === targetId || toId === targetId;
        const upstreamEdge = upstream.has(fromId) && upstream.has(toId);
        const downstreamEdge = downstream.has(fromId) && downstream.has(toId);
        if (involvesTarget) {{
          path.classList.add("visible", "direct");
        }} else if (upstreamEdge || downstreamEdge) {{
          path.classList.add("visible", "indirect");
        }}
      }});
    }}

    function registerHoverHandlers() {{
      columnElements.forEach((el, id) => {{
        el.addEventListener("mouseenter", () => highlightLineage(id));
        el.addEventListener("mouseleave", clearHighlights);
        el.addEventListener("focus", () => highlightLineage(id));
        el.addEventListener("blur", clearHighlights);
      }});
    }}

    buildTables();
    buildConnections();
    registerHoverHandlers();
  </script>
</body>
</html>
"""
