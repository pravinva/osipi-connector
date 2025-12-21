## Single-file merge (Hackathon / SDP Python Data Source)

Lakeflow SDP currently cannot reliably import multi-file Python modules for Python Data Source connectors.
For hackathon submission, the connector must be merged into a **single Python file** using the official
Lakeflow Community Connectors merge script.

This branch vendors the upstream script and required support files under `lakeflow_cc/`.

### What to run

From repo root:

```bash
cd lakeflow_cc
python3 scripts/merge_python_source.py osipi -o sources/osipi/_generated_osipi_python_source.py
python3 -m py_compile sources/osipi/_generated_osipi_python_source.py
```

### Output

The merged artifact is:

- `lakeflow_cc/sources/osipi/_generated_osipi_python_source.py`

This is the file you should use for SDP / Python Data Source deployment.

### Notes

- The upstream merge script does not handle multiline module docstrings cleanly, and `__future__` imports
  must be at the beginning of a file. For compatibility, `lakeflow_cc/sources/osipi/osipi.py` uses
  header comments (not a module docstring) and avoids `from __future__ import ...`.
