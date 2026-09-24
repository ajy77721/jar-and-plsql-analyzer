#!/usr/bin/env bash
# amp-ops.sh — one-stop operations script for the amp-analyzer staging/prod deployment.
#
# Usage:
#   amp-ops.sh logs [lines]        tail the app log (default 100; -f to follow: amp-ops.sh follow)
#   amp-ops.sh follow              stream the app log live
#   amp-ops.sh tree [path]         show the pod's /data folder structure (default /data)
#   amp-ops.sh clean-logs [days]   delete rolled log files older than N days (default 7; keeps the live log)
#   amp-ops.sh health              pod status, restarts, readiness, recent events, disk usage
#   amp-ops.sh restart             restart the deployment (new rollout)
#
# Flags: --prod targets the production cluster (default: staging)
set -e

MODE=staging
[ "$1" = "--prod" ] && { MODE=prod; shift; }
KUBECTL="$HOME/bin/kubectl"
[ -x /usr/local/bin/kubectl ] && KUBECTL=/usr/local/bin/kubectl
KC="$HOME/.kube/config.$MODE"
NS=amp-delivery
APP_LABEL=amp-analyzer
[ -f "$KC" ] || { echo "No kubeconfig at $KC — run: kanopy-oidc kube setup $MODE && kanopy-oidc kube login"; exit 1; }
K="$KUBECTL --kubeconfig $KC"

POD=$($K -n $NS get pods -o name 2>/dev/null | grep "amp-analyzer" | tail -1 | cut -d/ -f2)

case "$1" in
  logs)
    $K -n $NS logs "${POD:-NO_POD}" -c amp-analyzer --tail="${2:-100}"
    ;;
  follow)
    $K -n $NS logs -f "${POD:-NO_POD}" -c amp-analyzer --tail=50
    ;;
  tree)
    $K -n $NS exec "${POD:-NO_POD}" -c amp-analyzer -- sh -c "find \${DATA_DIR:-/data} -maxdepth 3 2>/dev/null | sort | head -80; echo '--- sizes ---'; du -sh \${DATA_DIR:-/data}/* 2>/dev/null"
    ;;
  clean-logs)
    DAYS="${2:-7}"
    echo "Deleting rolled log archives older than $DAYS day(s) in $MODE pod $POD:"
    $K -n $NS exec "${POD:-NO_POD}" -c amp-analyzer -- sh -c \
      "find \${DATA_DIR:-/data}/logs -name '*.gz' -mtime +$DAYS -print -delete 2>/dev/null; find \${DATA_DIR:-/data}/logs -name '*.log.*' -mtime +$DAYS -print -delete 2>/dev/null" || echo "nothing to clean"
    ;;
  health)
    echo "═══ amp-analyzer health ($MODE) ═══"
    echo "── pods ──"
    $K -n $NS get pods 2>&1 | grep -E "NAME|$APP_LABEL"
    echo "── restarts & readiness ──"
    $K -n $NS get pods -o jsonpath='{range .items[*]}{.metadata.name}: ready={.status.containerStatuses[0].ready} restarts={.status.containerStatuses[0].restartCount} started={.status.startTime}{"\n"}{end}' 2>/dev/null | grep "$APP_LABEL" || true
    echo "── recent events ──"
    $K -n $NS get events --sort-by=.lastTimestamp 2>/dev/null | grep "$APP_LABEL" | tail -5 || echo "(none)"
    echo "── disk usage (/data) ──"
    $K -n $NS exec "${POD:-NO_POD}" -c amp-analyzer -- sh -c "du -sh \${DATA_DIR:-/data} 2>/dev/null; df -h /data 2>/dev/null | tail -1" || echo "(pod not running)"
    echo "── app self-report ──"
    echo "(open https://amp-analyzer.$NS.$MODE.corp.mongodb.com/api/meta/deployment for region/latency)"
    ;;
  restart)
    $K -n $NS rollout restart deployment/amp-analyzer-web-app 2>/dev/null || $K -n $NS delete pods -l app=amp-analyzer
    echo "rollout triggered"
    ;;
  *)
    grep -E "^#   amp-ops" "$0" | sed 's/^#   //'
    ;;
esac
