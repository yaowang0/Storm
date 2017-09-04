什么是DRPC？
- RPC(Remote Procedure Call Protocol)——远程过程调用协议
- Distributed RPC:rpc请求流式、并行处理
- RPC请求参数当做输入流，结果当做输出流
- 利用Storm的分布式进行处理机制和能力
- 借助DPRC Server接受请求、返回响应

Storm只能获取数据，不能接收请求和发响应，借助一个DPRC Server来帮助完成。<br>

DPRC服务器协调：
1. 接受RPC请求
2. 发送请求到Storm topology
3. 从Storm topology接受结果
4. 把结果返回给客户端

修改storm.yaml中的drpc.servers配置<br>
cd /root/apache-storm-0.9.5<br>
vim conf/storm.yaml<br>
```
drpc.servers:
 - "master"
 - "slave1"
```
配置了2个，启动2个。<br>
启动DRPC Server:<br>
./bin/storm drpc >> ./logs/drpc.out 2>&1 &<br>
tail -f ./logs/drpc.log<br>


DRPC代码就100行，使用thrift接口，execute(给client调的)，fetch(给Spout调用)，result(给return bolt调的)<br>
```
DRPCClient client = new DRPCClient("master", 3772);
String result = client.execute("SpoutName", "abcd!");//abcd!为发送的数据
```