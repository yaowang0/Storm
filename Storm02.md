部署依赖：
- Java 6.+
- Python 2.6.6+

部署Zookeeper:
- 3.4.5+
- ZK为什么要用3.4.5，因为它支持磁盘的快照和namenode的定期删除，避免磁盘被打满。

分发Storm包：
- 0.9.4+

配置Storm：
- 修改storm.yaml配置文件

启动Storm<br>

Yet Another Markup Language，它有点像XML，和XML不同的是，XML是结构化的配置，它方便机器去解析，但是人读起来比价费劲。yaml，是一种比较友好的配置文件，机器容易解析，人读起来方便。<br>

在192.168.57.4，192.168.57.5，192.168.57.6三台机器部署Storm：<br>
tar -zxvf apache-storm-0.9.4.tar.gz<br>
cd apache-storm-0.9.4<br>
cd conf<br>
vim storm.yaml<br>
```
storm.zookeeper.severs:
- "master"
- "slave1"
- "slave2"

nimbus.host:"master"
```
启动Zookeeper:<br>
zkServer.sh start<br>

启动Nimbus:<br>
./bin/storm nimbus >> logs/nimbus.out 2>&1 &<br>
tail -f logs/nimbus.log<br>

启动UI:<br>
./bin/storm ui >> logs/ui.out 2>&1 &<br>
tail -f logs/ui.log<br>

启动supervisor:<br>
./bin/storm supervisor >> logs/supervisor.out 2>&1 &<br>
tail -f logs/supervisor.log<br>

启动logviewer<br>
./bin/storm logviewer >> logs/logviewer.out 2>&1 &<br>
tail -f logs/logviewer.log<br>

验证：浏览器打开WebUI, http://master:8080<br>

启动topology:<br>
./bin/storm jar examples/storm-starter/storm-startertopologies-0.9.5.jar storm.starter.WordCountTopology wordcount<br>