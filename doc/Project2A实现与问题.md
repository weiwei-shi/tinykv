# Project2A RaftKV

## 实现

在这一部分中，将实现基本的 raft 算法。

需要看`raft`目录下的几个文件：

- `raft.go`：主要要补充的文件，实现了Raft算法
- `log.go`：Raft日志的实现
- `raft_paper_test.go`：测试用例
- `raft_test.go`：测试用例
- `proto/pkg/eraftpb/eraft.pb.go`：一些协议值
- `rawnode.go`：2ac接口的实现
- `rawnode_test.go`：2ac测试用例

【**tips**】

* 在raft.Raft、raft.RaftLog、raft.RawNode和eraftpb.proto上添加你需要的任何状态。
* 测试假设第一次启动的筏子应该有0个任期。
* 测试假设新当选的领导者应该在其任期内追加一个noop条目。
* 测试假设，一旦领导者推进其提交索引，它将通过MessageType_MsgAppend消息广播提交索引。
* 测试并没有为本地消息、MessageType_MsgHup、MessageType_MsgBeat和MessageType_MsgPropose设置期限。
* 领导者和非领导者之间的日志条目附加是相当不同的，有不同的来源、检查和处理，要注意这一点。
* 不要忘了选举超时在peers之间应该是不同的。
* rawnode.go中的一些封装函数可以用raft.Step(local message)实现。
* 当启动一个新的raft时，从Storage中获取最后的稳定状态来初始化raft.Raft和raft.RaftLog。

### 1.领导人选举

为了实现领袖选举，需要从raft.Raft.tick()开始，它被用来将内部逻辑时钟提前一个刻度，从而驱动选举超时或心跳超时。现在不需要关心消息的发送和接收逻辑。如果你需要发送消息，只需将其推送到raft.Raft.msgs，raft收到的所有消息将被传递到raft.Raft.Step()。测试代码将从raft.Raft.msgs获取消息，并通过raft.Raft.Step()传递响应消息。raft.Raft.Step()是消息处理的入口，你应该处理像MsgRequestVote、MsgHeartbeat这样的消息及其响应。也需实现测试存根函数，并让它们被正确调用，如raft.Raft.becomeXXX，当筏子的角色改变时，它被用来更新筏子的内部状态。

### 2.日志复制

要实现日志复制，可能需要先在发送方和接收方处理 MsgAppend 和 MsgAppendResponse。 查看`raft/log.go` 中的 raft.RaftLog 是一个帮助管理 raft 日志的辅助结构体，在这里还需要通过 raft/storage.go 中定义的 Storage 接口与上层应用进行交互来获取持久化的数据(像日志条目和快照)。

### 3.原始节点接口

raft/rawnode.go 中的 raft.RawNode 是我们与上层应用交互的接口，raft.RawNode 包含 raft.Raft 并提供了一些包装函数，如 RawNode.Tick() 和 RawNode.Step()。它还提供 RawNode.Propose() 让上层应用程序提出新的 raft 日志。

另一个重要的结构 Ready 也在这里定义。在处理消息或推进逻辑时钟时，raft.Raft 可能需要与上层应用交互。

但这些交互不会立即发生，而是封装在 Ready 中，并由 RawNode.Ready() 返回给上层应用程序。何时调用 RawNode.Ready() 并处理它取决于上层应用程序。上层应用处理完返回的 Ready 后，还需要调用 RawNode.Advance() 等函数来更新 raft.Raft 的内部状态，如应用索引、稳定日志索引等。



##  测试

project2A的测试全部通过

![Project2aaTest](/images/Project2aaTest.jpg)

![Project2abTest](/images/Project2abTest.jpg)

![Project2acTest](/images/Project2acTest.jpg)



## 问题

#### 1.`MessageType_MsgBeat`和`MessageType_MsgHeartbeat`两个信号类型有什么区别？

解决：`MessageType_MsgBeat`：领导者告诉自己需要发送'MessageType_MsgHeartbeat'类型的心跳给他的跟随者们

`MessageType_MsgHeartbeat`：从领导者发送心跳到他的追随者的信号



#### 2.Ready结构体中Entries和CommittedEntries的含义

Entries指的是在发送消息之前需要保存到稳定存储的表项，即未持久化的日志条目，即stabled之后的entry；

CommittedEntries指的是要提交到状态机的条目，即已提交但还未应用到状态机的条目，即applied到 commited之间所有的 entry。

