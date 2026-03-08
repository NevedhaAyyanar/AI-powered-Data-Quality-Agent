"""
Fabric Notebook Converter
=========================
Converts between Fabric's .py notebook format and standard .ipynb format,
preserving all Fabric-specific markers for clean round-trip conversion.

Usage:
    python fabric_nb_convert.py to-notebook <file.py>     → Convert Fabric .py to .ipynb
    python fabric_nb_convert.py to-python <file.ipynb>     → Convert .ipynb back to Fabric .py

The .ipynb is for local editing in VS Code. The .py is what you push to Git for Fabric sync.
"""

import json
import sys
import os


def parse_fabric_py(py_path):
    """Parse Fabric .py file into structured blocks."""

    with open(py_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    lines = [l.rstrip("\n").rstrip("\r") for l in lines]

    if not lines or lines[0].strip() != "# Fabric notebook source":
        print("❌ Not a Fabric notebook. First line must be '# Fabric notebook source'")
        return None

    blocks = []
    i = 0

    # Block 0: prologue
    blocks.append({"type": "prologue", "raw": lines[0]})
    i = 1

    while i < len(lines) and lines[i].strip() == "":
        i += 1

    # Parse notebook-level METADATA + META block
    if i < len(lines) and "METADATA" in lines[i]:
        meta_header = lines[i]
        i += 1
        while i < len(lines) and lines[i].strip() == "":
            i += 1

        meta_lines = []
        while i < len(lines) and lines[i].strip().startswith("# META"):
            meta_lines.append(lines[i])
            i += 1

        blocks.append({
            "type": "notebook_metadata",
            "header": meta_header,
            "meta_lines": meta_lines
        })

    while i < len(lines) and lines[i].strip() == "":
        i += 1

    # Parse cells
    while i < len(lines):
        if lines[i].strip() == "# CELL ********************":
            cell_marker = lines[i]
            i += 1

            while i < len(lines) and lines[i].strip() == "":
                i += 1

            # Check for cell-level METADATA
            cell_meta_header = None
            cell_meta_lines = []
            if i < len(lines) and "METADATA" in lines[i]:
                cell_meta_header = lines[i]
                i += 1
                while i < len(lines) and lines[i].strip() == "":
                    i += 1
                while i < len(lines) and lines[i].strip().startswith("# META"):
                    cell_meta_lines.append(lines[i])
                    i += 1

            while i < len(lines) and lines[i].strip() == "":
                i += 1

            # Collect code lines until next CELL or end
            code_lines = []
            while i < len(lines) and lines[i].strip() != "# CELL ********************":
                code_lines.append(lines[i])
                i += 1

            while code_lines and code_lines[-1].strip() == "":
                code_lines.pop()

            blocks.append({
                "type": "cell",
                "cell_marker": cell_marker,
                "meta_header": cell_meta_header,
                "meta_lines": cell_meta_lines,
                "code_lines": code_lines
            })
        else:
            i += 1

    return blocks


def fabric_py_to_ipynb(py_path):
    """Convert Fabric's .py format to .ipynb"""

    blocks = parse_fabric_py(py_path)
    if blocks is None:
        return

    notebook = {
        "cells": [],
        "metadata": {
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "name": "synapse_pyspark"
            },
            "language_info": {"name": "python"},
            "fabric_blocks": json.loads(json.dumps(blocks))
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

    for block in blocks:
        if block["type"] != "cell":
            continue

        source = "\n".join(block["code_lines"])

        cell_meta = {}
        if block["meta_lines"]:
            meta_json_str = "\n".join(
                l.replace("# META", "", 1).strip()
                for l in block["meta_lines"]
            )
            try:
                cell_meta = json.loads(meta_json_str)
            except json.JSONDecodeError:
                pass

        cell = {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {"fabric": cell_meta},
            "outputs": [],
            "source": source
        }
        notebook["cells"].append(cell)

    ipynb_path = py_path.replace(".py", ".ipynb")
    with open(ipynb_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=1, ensure_ascii=False)

    print(f"✅ Converted: {py_path} → {ipynb_path}")
    print(f"   {len(notebook['cells'])} cells created")


def _default_cell_meta():
    """Default Fabric cell metadata for new cells."""
    return {
        "meta_header": "# METADATA ********************",
        "meta_lines": [
            '# META {',
            '# META   "language": "python",',
            '# META   "language_group": "synapse_pyspark"',
            '# META }'
        ]
    }


def _write_cell_block(output_lines, source, meta_header=None, meta_lines=None):
    """Write a single cell block in Fabric format."""
    output_lines.append("# CELL ********************")
    output_lines.append("")
    if meta_header:
        output_lines.append(meta_header)
        output_lines.append("")
        output_lines.extend(meta_lines)
        output_lines.append("")
    output_lines.append(source)
    output_lines.append("")


def ipynb_to_fabric_py(ipynb_path):
    """Convert .ipynb back to Fabric's .py format.
    Handles added, removed, and edited cells."""

    with open(ipynb_path, "r", encoding="utf-8") as f:
        notebook = json.load(f)

    fabric_blocks = notebook.get("metadata", {}).get("fabric_blocks", None)
    nb_cells = notebook.get("cells", [])

    # Get original cell blocks for metadata reuse
    original_cell_blocks = []
    if fabric_blocks:
        original_cell_blocks = [b for b in fabric_blocks if b["type"] == "cell"]

    output_lines = []

    # Write prologue
    if fabric_blocks:
        for block in fabric_blocks:
            if block["type"] == "prologue":
                output_lines.append(block["raw"])
                output_lines.append("")
            elif block["type"] == "notebook_metadata":
                output_lines.append(block["header"])
                output_lines.append("")
                output_lines.extend(block["meta_lines"])
                output_lines.append("")
    else:
        output_lines.append("# Fabric notebook source")
        output_lines.append("")

    # Write cells — match notebook cells to original blocks
    for i, cell in enumerate(nb_cells):
        source = cell.get("source", "")
        if isinstance(source, list):
            source = "".join(source)

        if i < len(original_cell_blocks):
            # Existing cell — reuse original Fabric metadata
            orig = original_cell_blocks[i]
            _write_cell_block(
                output_lines, source,
                meta_header=orig.get("meta_header"),
                meta_lines=orig.get("meta_lines", [])
            )
        else:
            # New cell — use default Fabric metadata
            defaults = _default_cell_meta()
            _write_cell_block(
                output_lines, source,
                meta_header=defaults["meta_header"],
                meta_lines=defaults["meta_lines"]
            )

    content = "\n".join(output_lines)

    py_path = ipynb_path.replace(".ipynb", ".py")
    with open(py_path, "w", encoding="utf-8") as f:
        f.write(content)

    # Summary
    added = max(0, len(nb_cells) - len(original_cell_blocks))
    removed = max(0, len(original_cell_blocks) - len(nb_cells))
    msg = f"✅ Converted: {ipynb_path} → {py_path}\n   {len(nb_cells)} cells"
    if added:
        msg += f" ({added} new)"
    if removed:
        msg += f" ({removed} removed)"
    print(msg)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(0)

    command = sys.argv[1]
    file_path = sys.argv[2]

    if command == "to-notebook":
        fabric_py_to_ipynb(file_path)
    elif command == "to-python":
        ipynb_to_fabric_py(file_path)
    else:
        print(__doc__)
