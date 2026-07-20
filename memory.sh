#!/usr/bin/env bash
# memory.sh — inject live project state for EasySubs-API
# Run: bash ./memory.sh from within the project folder
# The LLM runs this via Bash tool at session start.

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_NAME="$(basename "$PROJECT_DIR")"

echo "================================================================"
echo " SESSION CONTEXT — $(date)"
echo " PROJECT: $PROJECT_NAME"
echo "================================================================"
echo ""

# --- Git State ---
if [ -d "$PROJECT_DIR/.git" ]; then
  echo "--- Git — $PROJECT_NAME ---"
  cd "$PROJECT_DIR" || exit
  echo "Branch:        $(git branch --show-current 2>/dev/null || echo '(unknown)')"
  echo ""
  echo "Last 5 commits:"
  git log --oneline -5 2>/dev/null || echo "  (no commits yet)"
  echo ""
  echo "Uncommitted changes:"
  git status --short 2>/dev/null || echo "  (none)"
  echo ""
else
  echo "GIT: No repository. Initialize with: git init"
  echo ""
fi

# --- Primer ---
if [ -f "$PROJECT_DIR/primer.md" ]; then
  echo "--- Primer ($PROJECT_NAME) ---"
  cat "$PROJECT_DIR/primer.md"
  echo ""
else
  echo "PRIMER: No primer.md found."
  echo ""
fi

# --- Wiki ---
if [ -d "$PROJECT_DIR/wiki" ]; then
  echo "--- Wiki ---"
  if [ -f "$PROJECT_DIR/wiki/index.md" ]; then
    echo "Index:"
    head -50 "$PROJECT_DIR/wiki/index.md"
    echo ""
  fi
  if [ -f "$PROJECT_DIR/wiki/log.md" ]; then
    echo "Recent log entries:"
    head -50 "$PROJECT_DIR/wiki/log.md"
    echo ""
  fi
else
  echo "WIKI: No wiki/ folder found."
  echo ""
fi

echo "================================================================"