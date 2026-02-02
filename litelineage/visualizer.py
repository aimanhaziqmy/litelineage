import sqlite3
import os

TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<body>
  <pre class="mermaid">
    graph LR;
    %% Node Definitions
    {node_definitions}
    
    %% Edge Definitions
    {edge_definitions}
  </pre>
  <script type="module">
    import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
    mermaid.initialize({{ startOnLoad: true }});
  </script>
</body>
</html>
"""

def generate_graph(db_path="lineage.db", output_file="lineage.html"):
    if not os.path.exists(db_path):
        print("No lineage database found. Run your pipeline first!")
        return

    # 1. Fetch Data
    raw_edges = set() # Use a set to remove duplicates
    all_node_names = set()
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("SELECT input_asset, process_name, output_asset FROM lineage")
        for row in cursor:
            src, process, dst = row
            all_node_names.add(src)
            all_node_names.add(dst)
            raw_edges.add((src, process, dst))

    # 2. Create "Safe IDs" for every node
    # Map "api:stripe/orders" -> "node_0"
    # Map "raw/users.csv"     -> "node_1"
    node_map = {name: f"node_{i}" for i, name in enumerate(all_node_names)}

    # 3. Build Mermaid Strings
    # Define Nodes: node_0["api:stripe/orders"]
    node_defs = []
    for name, safe_id in node_map.items():
        # Escape quotes in names just in case
        safe_name = name.replace('"', "'")
        node_defs.append(f'    {safe_id}["{safe_name}"]')
    
    # Define Edges: node_0 -->|ingest| node_1
    edge_defs = []
    for src, process, dst in raw_edges:
        src_id = node_map[src]
        dst_id = node_map[dst]
        edge_defs.append(f'    {src_id} -->|{process}| {dst_id}')

    # 4. Render to HTML
    html = TEMPLATE.format(
        node_definitions="\n".join(node_defs),
        edge_definitions="\n".join(edge_defs)
    )
    
    with open(output_file, "w") as f:
        f.write(html)
    
    print(f"Graph generated: {os.path.abspath(output_file)}")