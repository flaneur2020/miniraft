a copycat implementation to learn raft

todo

- [x] 异步响应 append log
- [x] 使用内存状态替代 storage 的读写
- [x] 简化 storage 对象, 干掉 storage 中的 commitIndex
- [x] 异步 runElection
- [x] 计算 commitIndex
- [x] appendEntriesReply 处理
- [ ] apply 逻辑
- [ ] Node.start
- [ ] 确认 requestVote 和 appendEntries 逻辑
- [ ] lock

references

https://github.com/codymalick/simple-raft
https://github.com/radondb/xenon/blob/master/src/raft/raft.go
https://github.com/streed/simpleRaft
https://github.com/Qihoo360/floyd/tree/master/floyd/src
https://github.com/hashicorp/raft/blob/c95aa91e604eaafe4174d7ae0f404afa247f60c4/raft.go
https://zhuanlan.zhihu.com/p/27415397
