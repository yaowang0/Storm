Hadoop MapReduce并发模型：
- 每个map task就是一个进程，每个reduce task也是一个进程，每个机器上都有很多的task进程，task进程里面就是mapreduce函数。

Storm并发模型：5个层次
- Cluster：集群
- Supervisor：对应就是一个个host，就是一个个的node，就是一个机器级别的，然后一个机器有很多的worker。
- Worker：对应的process级别，就是进程级别的，机器上跑几个进程，规定4个worker，就4个进程，每个worker里面跑一个个的executor。
- Executor：对应着一个个线程，一个线程运行着一个或者多个task，一般很多情况下就一个task。
- task：最低级别的object，就是线程操作的就是一个个对象。<br>
![avatar](example-of-a-running-topology.png)
```
Config conf = new Config();
conf.setNumWorkers(2); // use two worker processes

topologyBuilder.setSpout("blue-spout", new BlueSpout(), 2); // set parallelism hint to 2

topologyBuilder.setBolt("green-bolt", new GreenBolt(), 2)
               .setNumTasks(4)
               .shuffleGrouping("blue-spout");

topologyBuilder.setBolt("yellow-bolt", new YellowBolt(), 6)
               .shuffleGrouping("green-bolt");

StormSubmitter.submitTopology(
        "mytopology",
        conf,
        topologyBuilder.createTopology()
    );
```

数据传输：
1. executor执行spout.nextTuple()/bolt.execute(),emit tuple放入executor transfer queue
2. executor transfer thread把自己transfer queue里面的tuple放到worker transfer queue
3. worker transfer thread把transfer queue里面的tuple序列化发送到远程的worker
4. worker receiver thread分别从网络收数据，反序列化成tuple放到对应的executor的receive queue
5. executor receiver thread从自己的receive queue取出tuple，调用bolt.execute()

Storm无状态：
- nimbus/supervisor/worker状态都存在ZK里面，少数worker信息存在本地文件系统里面，还有nimbus把程序的JAR包也存在本地系统里面，它的好处是甚至nimbus挂掉之后，都可以快速的恢复，恢复就是知道重新起一个然后去ZK里面读取相应的数据，因为这样所以storm非常的稳定，非常的健壮，其实通过zk做了一个很好的解耦，任何模块挂了之后都还可以进行很好的工作，也就是譬如说supervisor挂了，worker一样可以照常工作，因为supervisor和worker之间没有直接的关系，都是通过zookeeper或本地文件进行状态的传输的。

Storm单中心：
- master/slave的架构，master是nimbus，slave是supervisor，单中心的好处是，使得整个topology的调度变得非常的简单，因为只有一个中心的话，就是决定都有一个人来决定，不会出现仲裁的情况。
- master这种也会有单中心的问题，就是单点失效的问题，当然这个问题也不是很大，当没有新的topology发送上来的时候，运行在集群里面的topology不会有什么影响，当然极端的情况就是，这时候某个supervisor机器挂掉了，没有nimbus了就不会做重新的调度，这样还是会有一定的影响。
- 另外一个单点的问题，就是单点性能的问题，因为现在topology的jar包还是通过nimbus来分发的，客户端提交还是先提交到nimbus，然后各个supervisor去nimbus上面去下载，当文件比较大的时候，supervisor机器比较多的时候，nimbus还是会成为瓶颈，网卡很快会被打满，当然为了解决这个问题呢，storm也有一些办法，比如说把文件的分发变成P2P的。

Storm隔离性好：
- 真正干活的是executor和worker，nimbus/supervisor就起到一个控制topology流程的作用，数据流程都在executor和worker里面来做，所有的数据传输都是在worker之间完成的，不需要借助于nimbus和supervisor，特别是不需要借助于supervisor，这点设计上就是非常成功的，Storm所有的数据传输都在worker间，即使supervisor挂了，数据传输在worker之间，supervisor挂了之后，一点不会受影响，更不可思议的是，nimbus/supervisor全部挂掉了话，worker还能正常工作，这个在hadoop中是不可想象的，所有的master/slave都挂了，task还能正常运行。

Storm的ack机制：
- Tuple树
- 在Storm的一个Topology中，Spout通过SpoutOutputCollector的emit()方法发射一个tuple(源)即消息。而后经过在该Topology中定义的多个Bolt处理时，可能会产生一个或多个新的Tuple。源Tuple和新产生的Tuple便构成了一个Tuple树。当整棵树被处理完成，才算一个Tuple被完全处理，其中任何一个节点Tuple处理失败或超时，则整棵树失败。
- 可靠性指的是Storm保证每个tuple都能被topoloyg完全处理。而且处理的结果要么成功要么失败。出现失败的原因可能有两种：节点处理失败或者处理超时。
- Storm的Bolt有BasicBolt和RichBolt，在BasicBolt中，BasicOutputCollector在emit数据的时候，会自动和输入的tuple相关联，而在execute方法结束的时候那个输入tuple会被自动的ack(有一定的条件)。
- 在使用RichBolt时要实现ack，则需要在emit数据的时候，显示指定该数据的源tuple，即collector.emit(oldTuple, newTuple)且需要在execute执行成功后调用源tuple的ack进行ack。
- 如果可靠性不是那么重要，不太在意在一些失败的情况下损失一些数据，那么可以通过不跟踪这些tuple树来获取更好的性能。

有三种方法可以去掉可靠性：
- 第一是把Config.TOPOLOGY_ACKERS设置成0. 在这种情况下，storm会在spout发射一个tuple之后马上调用spout的ack方法。也就是说这个tuple树不会被跟踪。
- 第二是在tuple层面去掉可靠性。可以在发射tuple的时候不指定messageid来达到不跟踪某个特定的spout tuple的目的。
- 最后一个方法是如果对于一个tuple树里面的某一部分到底成不成功不是很关心，那么可以在发射这些tuple的时候unanchor它们。这样这些tuple就不在tuple树里面，也就不会被跟踪了。

Acker的跟踪算法是Storm的主要突破之一，对任意大的一个Tuple树，它只需要恒定的20字节就可以进行跟踪。<br>
cker跟踪算法的原理：acker对于每个spout-tuple保存一个ack-val的校验值，它的初始值是0，然后每发射一个Tuple或Ack一个Tuple时，这个Tuple的id就要跟这个校验值异或一下，并且把得到的值更新为ack-val的新值。那么假设每个发射出去的Tuple都被ack了，那么最后ack-val的值就一定是0。Acker就根据ack-val是否为0来判断是否完全处理，如果为0则认为已完全处理。