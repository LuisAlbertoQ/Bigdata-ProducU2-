"""
Export Jupyter notebooks to Markdown for MkDocs documentation.
Captures code, markdown, and text outputs.
"""

from __future__ import annotations
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_DIR = ROOT / "notebooks"
DOCS_DIR = ROOT / "docs"

NOTEBOOKS = [
    "01_exploracion_supabase.ipynb",
    "02_verificacion_kafka.ipynb",
    "03_streaming_pipeline.ipynb",
    "04_modelo_random_forest.ipynb",
    "05_modelo_mlp.ipynb",
    "06_modelo_xgboost.ipynb",
    "07_modelo_lstm.ipynb",
    "08_comparacion_modelos.ipynb",
]


def cell_source(cell: dict) -> str:
    src = cell.get("source", "")
    return "".join(src) if isinstance(src, list) else src


def convert_notebook(path: Path) -> str:
    nb = json.loads(path.read_text(encoding="utf-8-sig"))
    out = []
    for cell in nb.get("cells", []):
        src = cell_source(cell).rstrip()
        if not src.strip():
            continue
        if cell["cell_type"] == "markdown":
            out.append(src)
        elif cell["cell_type"] == "code":
            out.append(f"```python\n{src}\n```")
            outputs = cell.get("outputs", [])
            for o in outputs:
                if o.get("output_type") == "stream":
                    text = "".join(o.get("text", []))
                    if text.strip():
                        out.append(f"```\n{text}\n```")
                elif o.get("output_type") == "execute_result":
                    data = o.get("data", {})
                    if "text/plain" in data:
                        text = "".join(data["text/plain"])
                        if text.strip():
                            out.append(f"```\n{text}\n```")
    return "\n\n".join(out).rstrip() + "\n"


def main() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    for nb_name in NOTEBOOKS:
        nb_path = NOTEBOOKS_DIR / nb_name
        if not nb_path.exists():
            print(f"WARNING: {nb_path} not found, skipping")
            continue
        md_name = nb_name.replace(".ipynb", ".md")
        (DOCS_DIR / md_name).write_text(convert_notebook(nb_path), encoding="utf-8")
        print(f"OK: {nb_name} -> {md_name}")

    print("Done. All notebooks exported.")


if __name__ == "__main__":
    main()
