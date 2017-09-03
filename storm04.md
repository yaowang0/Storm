Storm计算模型：
- DAG计算模型，一个阶段接着另一个阶段，在这个有向无环图里面可以灵活的组合，DAG是由Spout和Bolt组合起来的，它们都是节点，边就是stream数据流，数据流里面的数据单元就是Tuple。


Spout:
- open()
1. 初始化的时候调用。
- nextTuple()
1. 每个线程不断调这个回调函数，Spout主动去Kafka取数据，然后在用emit()生成一个Tuple给后面的Bolt进行处理。
2. 数据是不能打给Storm的，以为Storm是没有地方接这个数据的，Storm是主动拉来数据的系统，数据放在消息队列Kafka或者Storm自带的DRPC Server中。

Grouping它是一个路由策略，它决定一个Tuple发个下游某个bolt中n个并发中的哪个task。<br>
Storm里面有7种类型的Stream Grouping，也可以通过实现CustomStreamGrouping接口来实现自定流的分组。
- **Shuffle Grouping**：随机分组，随机派发stream里面的tuple，保证每个bolt task接收到的tuple数目大致相同。

- **Fields Grouping**：按字段分组，比如，按"user-id"这个字段来分组，那么具有同样"user-id"的tuple 会被分到相同的Bolt里的一个task，而不同的"user-id"则可能会被分配到不同的task。

- All Grouping：广播发送，对于每一个tuple，所有的bolts都会收到

- Global Grouping：全局分组，整个stream被分配到storm中的一个bolt的其中一个task。再具体一点就是分配给id值最低的那个task。

- None Grouping：不分组，这个分组的意思是说stream不关心到底怎样分组。目前这种分组和Shuffle grouping是一样的效果，有一点不同的是storm会把使用none grouping的这个bolt放到这个bolt的订阅者同一个线程里面去执行（如果可能的话）。

- **Direct Grouping**：指向型分组，这是一种比较特别的分组方法，用这种分组意味着消息（tuple）的发送者指定由消息接收者的哪个task处理这个消息。只有被声明为Direct Stream 的消息流可以声明这种分组方法。而且这种消息tuple必须使用emitDirect方法来发射。消息处理者可以通过TopologyContext来获取处理它的消息的task的id (OutputCollector.emit方法也会返回task的id)。

- Local or shuffle Grouping：本地或随机分组。如果目标bolt有一个或者多个task与源bolt的task在同一个工作进程中，tuple将会被随机发送给这些同进程中的tasks。否则，和普通的Shuffle Grouping行为一致。

