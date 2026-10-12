#!/bin/sh
set -u

pip install -q "bandit[toml]" pip-audit

status=0

echo "=== Bandit (SAST)"
bandit -c pyproject.toml -r services/control_panel -ll || status=1

echo "=== pip-audit (SCA)"
found_reqs=0
for f in $(find . -name requirements.txt -not -path "*/.venv/*" -not -path "*/node_modules/*"); do
  echo "== Проверяем $f"
  pip-audit -r "$f" || status=1
  found_reqs=1
done

if [ -f "pyproject.toml" ]; then
  echo "== Проверяем pyproject.toml"
  pip-audit -p pyproject.toml || status=1
fi

exit $status