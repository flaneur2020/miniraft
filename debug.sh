make

rm -rf /tmp/raft-debug/*

tmux \
	new "bin/miniraft --conf conf/sample/r1.json" ';' \
	split -p 66 "bin/miniraft --conf conf/sample/r2.json" ';' \
	split -p 50 "bin/miniraft --conf conf/sample/r3.json"
