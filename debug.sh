make

rm -rf /tmp/raft-debug/*

tmux \
	new "bin/miniraft --conf conf/sample/r1.json" ';' \
	split "bin/miniraft --conf conf/sample/r2.json" ';' \
	split "bin/miniraft --conf conf/sample/r3.json"
