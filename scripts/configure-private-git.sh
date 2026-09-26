#!/bin/sh
# Configure this clone so commits do not reveal personal information:
#
#   * a neutral author/committer identity (local to this repository)
#   * refuse to commit when no identity is configured (no user@hostname fallback)
#   * `git ci` = commit with UTC timestamps (no time zone / location hint)
#   * a pre-commit hook that blocks staged changes containing your home
#     directory path, your global git e-mail address, your host name, or any
#     string listed with `git config --local --add privacy.block "<text>"`
#
# Usage: sh scripts/configure-private-git.sh ["Display Name" "email@example.invalid"]
set -eu

name=${1:-"AzureSDK.jl contributors"}
email=${2:-"contributors@azuresdk-jl.invalid"}

git config --local user.name "$name"
git config --local user.email "$email"
git config --local user.useConfigOnly true
git config --local commit.gpgsign false
git config --local alias.ci '!TZ=UTC git commit'

hook="$(git rev-parse --git-path hooks)/pre-commit"
cat > "$hook" <<'HOOK'
#!/bin/sh
# Block commits whose staged changes contain personal data (see
# scripts/configure-private-git.sh). Bypass with `git commit --no-verify`.
added=$(git diff --cached -U0 --no-color | grep '^+' | grep -v '^+++' || true)
[ -z "$added" ] && exit 0
found=""
check() {
    [ -n "$1" ] && [ "$1" != "localhost" ] && printf '%s' "$added" | grep -F -i -q -- "$1" && found="$1"
}
check "${HOME:-}" || true
check "$(git config --global user.email 2>/dev/null || true)" || true
check "$(hostname 2>/dev/null || true)" || true
for p in $(git config --local --get-all privacy.block 2>/dev/null | tr ' ' '\001'); do
    check "$(printf '%s' "$p" | tr '\001' ' ')" || true
done
if [ -n "$found" ]; then
    echo "pre-commit: staged changes contain personal data (home path, e-mail, host name or privacy.block entry)." >&2
    echo "            Remove it or use 'git commit --no-verify' if this is intended." >&2
    exit 1
fi
exit 0
HOOK
chmod +x "$hook"
echo "Configured: $(git config user.name) <$(git config user.email)>; use 'git ci' for UTC commits; pre-commit privacy hook installed."
