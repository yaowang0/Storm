
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
import backtype.storm.Constants;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class PVTopology {
  public static class ParseBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      // format: 205.212.115.106 - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/countdown.html HTTP/1.0" 200 3985
      String line = tuple.getString(0);
      String[] splits = line.split(" ");
      if (splits.length > 6) {
        String time = splits[3];
        String url = splits[6];
        int index = url.indexOf("?");
        if (index > 0) {
          url = url.substring(0, index);
        }
        System.out.println(time + "\t" + url);
        collector.emit(new Values(System.currentTimeMillis() / (60 * 1000), url));
      } else {
        System.err.println("can not parse for log " + line);
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("time", "url"));
    }
  }

  public static class CountBolt extends BaseBasicBolt {

    TreeMap<Long, Map<String, Integer>> timeCounts = new TreeMap<Long, Map<String, Integer>>();

    private Jedis jedis;
    private String prefix;

    private void output(long minute, String url, Integer count) {
      for (int i = 0; i < 5; i++) {
        try {
          jedis.hset(prefix, new Date(minute * 60 * 1000) + "_" + url, count.toString());
          break;
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
      String host = "spark003";
      Integer port = 6379;
      if (stormConf.get("redis.host") != null) {
        host = (String) stormConf.get("redis.host");
      }
      if (stormConf.get("redis.port") != null) {
        port = (Integer) stormConf.get("redis.port");
      }
      System.out.println("connecting to redis " + host + ":" + port);
      this.jedis = new Jedis(host, port);
      System.out.println("connected to redis " + host + ":" + port);
      this.prefix = (String) stormConf.get("topology.name");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      long currentMinute = System.currentTimeMillis() / (60 * 1000);

      // handle timer tick
      if (tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
        // save counts
        Iterator<Map.Entry<Long, Map<String, Integer>>> iter = timeCounts
            .entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<Long, Map<String, Integer>> entry = iter.next();
          long minute = entry.getKey();
          if (currentMinute > minute + 1) {
            for (Map.Entry<String, Integer> counts : entry.getValue().entrySet()) {
              String url = counts.getKey();
              Integer count = counts.getValue();
              output(minute, url, count);
            }

            iter.remove();
          } else {
            break;
          }
        }
        return;
      }

      long minute = tuple.getLong(0);
      String url = tuple.getString(1);

      if (currentMinute > minute + 1) {
        System.out.println("drop outdated tuple " + tuple);
        return;
      }

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
      collector.emit(new Values(minute, url, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("time", "url", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      Config conf = new Config();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
      return conf;
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    // config kafka spout
	String topic = "nasa_weblog";
	ZkHosts zkHosts = new ZkHosts("192.168.80.201:2181,192.168.80.202:2181,192.168.80.203:2181");
	SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/MyKafka", // 偏移量offset的根目录
			"MyTrack");// 对应一个应用
	List<String> zkServers = new ArrayList<String>();
	System.out.println(zkHosts.brokerZkStr);
	for (String host : zkHosts.brokerZkStr.split(",")) {
		zkServers.add(host.split(":")[0]);
	}

	spoutConfig.zkServers = zkServers;
	spoutConfig.zkPort = 2181;
	spoutConfig.forceFromStart = true; // 从头开始消费
	spoutConfig.socketTimeoutMs = 60 * 1000;
	spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()); // 定义输出为String

	KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
	builder.setSpout("kafka_spout", kafkaSpout, 1);
    builder.setBolt("parse", new ParseBolt(), 2).shuffleGrouping("kafka_spout");
    builder.setBolt("count", new CountBolt(), 4).fieldsGrouping("parse", new Fields("time", "url"));

    Config conf = new Config();

    conf.setNumWorkers(3);
//    StormSubmitter.submitTopologyWithProgressBar(args[0] + "_pv", conf,
//        builder.createTopology());
    LocalCluster localCluster = new LocalCluster();
	localCluster.submitTopology(topic + "_pv", conf, builder.createTopology());
  }
}

