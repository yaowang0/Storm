/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.sxt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.BatchSubtopologyBuilder;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import storm.kafka.*;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class PVTopology2 {
  public static class PrepareBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      Object id = tuple.getMessageId().toString();
      collector.emit(new Values(id, tuple.getValue(0)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "data"));
    }
  }

  public static class ParseBolt extends BaseBasicBolt {
    private static SimpleDateFormat format = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss", Locale.US);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      Object id = tuple.getValue(0);
      String[] lines = tuple.getString(1).split("\n");

      for (String line : lines) {
        // log format:
        // 205.212.115.106 - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/countdown.html HTTP/1.0" 200 3985
        String[] splits = line.split(" ");
        if (splits.length > 6) {
          String time = splits[3];
          String url = splits[6];
          int index = url.indexOf("?");
          if (index > 0) {
            url = url.substring(0, index);
          }
          try {
            long minute = format.parse(time).getTime() / (60 * 1000);
            Values t = new Values(id, minute, url);
            collector.emit(t);
            System.out.println("emitted tuple " + t);
          } catch (Exception e) {
            System.err.println("can not parse time for log " + line);
          }
        } else {
          System.err.println("can not parse for log " + line);
        }
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "time", "url"));
    }
  }

  public static class BatchCountBolt extends BaseBatchBolt {
    Map stormConf;
    BatchOutputCollector _collector;
    Object _id;
    TreeMap<Long, Map<String, Integer>> timeCounts = new TreeMap<Long, Map<String, Integer>>();

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
      stormConf = conf;
      _collector = collector;
      _id = id;
    }

    @Override
    public void execute(Tuple tuple) {
      System.out.println("input tuple is " + tuple);
      long minute = tuple.getLong(1);
      String url = tuple.getString(2);

      Map<String, Integer> counts = timeCounts.get(minute);
      if (counts == null) {
        counts = new HashMap<String, Integer>();
        timeCounts.put(minute, counts);
      }
      Integer count = counts.get(url);
      if (count == null)
        count = 0;
      count++;
      counts.put(url, count);
    }

    @Override
    public void finishBatch() {
      System.out.println("finishBatch for id " + _id);

      String host = "spark001";
      Integer port = 6379;
      if (stormConf.get("redis.host") != null) {
        host = (String) stormConf.get("redis.host");
      }
      if (stormConf.get("redis.port") != null) {
        port = (Integer) stormConf.get("redis.port");
      }
      String key = (String) stormConf.get("topology.name");

      // save counts
      for (Map.Entry<Long, Map<String, Integer>> entry : timeCounts.entrySet()) {
        long minute = entry.getKey();
        for (Map.Entry<String, Integer> counts : entry.getValue().entrySet()) {
          String url = counts.getKey();
          Integer count = counts.getValue();
          RedisUtil.get(host, port).hincrBy(key, new Date(minute * 60 * 1000) + "_" + url, count);
        }
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "partial-count"));
    }
  }

  public static class RedisUtil {
    private static Map<String, RedisUtil> instanceMap = new HashMap<String, RedisUtil>();

    private Jedis jedis;
    private RedisUtil (String host, int port) {
      this.jedis = new Jedis(host, port);
      System.out.println("connect to redis " + host + " " + port);
    }

    public static synchronized RedisUtil get(String host, int port) {
      RedisUtil instance = instanceMap.get(host + ":" + port);
      if (instance == null) {
        instance = new RedisUtil(host, port);
        instanceMap.put(host + ":" + port, instance);
      }
      return instance;
    }

    public synchronized void hincrBy(String key, String field, long value) {
      boolean ok = false;
      for (int i = 0; i < 5; i++) {
        try {
          Long ret = jedis.hincrBy(key, field, value);
          System.out.println("hincrBy ok for " + key + " " + field + " " + value + " ret " + ret);
          ok = true;
          break;
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      if (!ok) {
        throw new RuntimeException("hincrBy failed for " + key + " " + field + " " + value);
      }
    }
  }


  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

 // config kafka spout
 	String topicName = "nasa_weblog";
    ZkHosts zkHosts = new ZkHosts("192.168.80.201:2181,192.168.80.202:2181,192.168.80.203:2181");
    SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topicName,// 偏移量offset的根目录
        "/" + topicName, UUID.randomUUID().toString());// 对应一个应用
    List<String> zkServers = new ArrayList<String>();
	System.out.println(zkHosts.brokerZkStr);
	for (String host : zkHosts.brokerZkStr.split(",")) {
		zkServers.add(host.split(":")[0]);
	}

	spoutConfig.zkServers = zkServers;
	spoutConfig.zkPort = 2181;
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
    builder.setSpout("kafka_spout", kafkaSpout, 2);

    builder.setBolt("prepareid", new PrepareBolt(), 4).shuffleGrouping("kafka_spout");

    BatchSubtopologyBuilder batchBuilder = new BatchSubtopologyBuilder("parse", new ParseBolt(), 4);
    batchBuilder.getMasterDeclarer().shuffleGrouping("prepareid");
    batchBuilder.setBolt("batchcount", new BatchCountBolt(), 8).fieldsGrouping("parse", new Fields("time", "url"));
    batchBuilder.extendTopology(builder);

    Config conf = new Config();

    conf.setNumWorkers(3);
//    StormSubmitter.submitTopologyWithProgressBar(args[0] + "_pv2", conf,
//        builder.createTopology());
    LocalCluster localCluster = new LocalCluster();
	localCluster.submitTopology(topicName + "_pv", conf, builder.createTopology());
  }
}
