# Shared by the claudeai/setup_<svc>.sh scripts that record a pidfile.
# stop_pidfile <name> <pidfile>: kill the recorded server, wait for it to exit
# so a start right after does not see the dying server as up, drop the pidfile.
stop_pidfile() {
  local pid
  if [ -s "$2" ] && pid="$(cat "$2")" && kill "$pid" 2>/dev/null; then
    for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  fi
  rm -f "$2"
  echo "[$1] stopped"
}
