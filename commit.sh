#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ -z $(git status --porcelain) ]]; then
  echo "Nothing to commit."
  exit 0
fi

git status --short

echo ""
read -rp "Commit message: " msg

if [[ -z "$msg" ]]; then
  echo "Aborted: empty commit message."
  exit 1
fi

git add -A
git commit -m "$msg"
git push
