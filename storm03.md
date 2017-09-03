Nimbus:
- 集群管理
- 调度topology
Supervisor:
- 启停worker
Worker:
- 一个JVM进程资源分配的单位
- 启动executor
Executor:
- 实际干活的线程
Zookeeper:
- 存储状态信息，调度信息，心跳

Nimbus:相当于master，Storm是master/slave的架构
- 负责两件事：
1. 负责管理集群，这些slave都向Zookeeper写信息，然后nimbus通过zk获取这些slave节点的信息，这样nimbus就知道集群里面有多少个节点，节点出于什么样的状态，都运行着什么样的任务。
2. 调度topology，当一个topology通过接口提交到集群上面之后，负责把topology里面的worker分配到supervisor上面去执行

Supervisor:每台机器会起一个supervisor进程，supervisor就是slave
- supervisor
1. 启停/监控worker，当nimbus调用之后，supervisor去把worker启动起来，这样的话worker就可以开始干活了。
2. 把自己的情况汇报出去

Worker:实际干活的，每个机器上面Supervisor会启动很多个Worker，每个机器会配置一定的Worker。
- 一个Worker，就是一个JVM，JVM干两件事情：
1. 启动Executor
2. 负责Worker与Worker之间的数据传输
3. 把自己的信息定期往本地系统写一份，然后再往ZK上面写一份

Executor:真正干活的是Worker里面的Executor
- worker启动executor，executor执行Spout里面的nextTuple()、Bolt执行execute()这些回调函数，是真正运行的线程。
1. 创建实际的Spout/Bolt的对象
2. 关键的两个线程：执行线程/传输线程

Storm使用ZK存储的信息：
|ZK路径|数据|
|------|----|
|/storm/supervisors/supervisor-id|supervisor心跳|
|/storm/storms/storm-id|topology基本信息|
|/storm/assignments/storm-id|topology调度信息|
|/storm/workerbeats/storm-id/node-port|worker心跳|
|/storm/errors/storm-id/component-id|spout/bolt错误信息|