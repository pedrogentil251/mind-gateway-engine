#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://github.com/pedrogentil251/mind-gateway-engine.git"

if grep -rnE "sk-or-v1-|ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}" . --exclude-dir=.git --exclude=push.sh; then
  echo "[ERRO] Chave/token encontrada. Remova antes de publicar!"
  exit 1
fi

git config --global credential.helper store
[ ! -d .git ] && git init
git branch -M main

if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$REPO_URL"
else
  git remote add origin "$REPO_URL"
fi

git add -A
git status --short

if ! git diff --cached --quiet; then
  git commit -m "feat: estrutura inicial open-source do Mind Gateway (v0.4.0 / MG-BASE-07)"
fi

if git ls-remote --exit-code --heads origin main >/dev/null 2>&1; then
  git pull --rebase origin main
fi

git push -u origin main
echo "[SUCESSO] Repositorio mind-gateway-engine publicado no GitHub!"
