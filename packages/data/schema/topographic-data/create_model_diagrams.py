import os
import sqlite3
import matplotlib.pyplot as plt


db_path = "c:/data/topoedit/topographic-data/topographic-data.gpkg"
output_dir = "c:/data/model/metadata_source/model-diagrams/png"

os.makedirs(output_dir, exist_ok=True)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [row[0] for row in cursor.fetchall()]

for table in tables:
    if table.startswith('rtree') or table.startswith('spatial_ref_sys'):
        continue
    cursor.execute(f"PRAGMA table_info('{table}')")
    cols = cursor.fetchall()

    lines = [f"{col[1]} ({col[2]})" for col in cols]
    text = f"{table}\n" + "\n".join(lines)

    max_chars = max(len(table), *(len(line) for line in lines)) if lines else len(table)
    fig_width = min(12, max(2.5, max_chars * 0.09))
    fig_height = max(1.0, (len(lines) + 1) * 0.22 + 0.3)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    ax.text(0.0, 1.0, text,
            va='top', ha='left',
            family='monospace',
            fontsize=10)

    ax.axis('off')
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)

    safe_table_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in table)
    output_file = os.path.join(output_dir, f"{safe_table_name}.png")
    fig.savefig(output_file, dpi=200, bbox_inches='tight', pad_inches=0.03)
    plt.close(fig)
    print(f"Saved: {output_file}")

conn.close()