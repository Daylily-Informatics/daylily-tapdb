daylily-tapdb
source ./tapdb_activate.sh
tapdb pg init dev
tapdb pg start-local dev
tapdb db setup dev
tapdb ui start
