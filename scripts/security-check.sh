#!/bin/sh
set -u
pip install -q "bandit[toml]" pip-audit

status=0

echo "=== Bandit"
bandit -c pyproject.toml -r control_panel -ll || status=1

echo "=== pip-audit"
for f in $(find . -name requirements.txt -not -path "*/.venv/*" -not -path "*/node_modules/*"); do
  echo "== $f"
  pip-audit -r "$f" || status=1
done

exit $status