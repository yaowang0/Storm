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
import backtype.storm.StormSubmitter;
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
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class LogFilterTopology {

	public static class FilterBolt extends BaseBasicBolt {
		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String line = tuple.getString(0);
			if (line.contains("ERROR")) {
				System.err.println(line);
				collector.emit(new Values(line));
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("message")); // 这个地方写message是给后面FieldNameBasedTupleToKafkaMapper来用
		}
	}

	public static void main(String[] args)  {
		TopologyBuilder builder = new TopologyBuilder();
		// config kafka spout
		String topic = "testflume";
		ZkHosts zkHosts = new ZkHosts("192.168.57.4:2181,192.168.57.5:2181,192.168.57.6:2181");
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/test", // 偏移量offset的根目录
				"test");// 对应一个应用
//		List<String> zkServers = new ArrayList<String>();
//		System.out.println(zkHosts.brokerZkStr);
//		for (String host : zkHosts.brokerZkStr.split(",")) {
//			zkServers.add(host.split(":")[0]);
//		}
//
//		spoutConfig.zkServers = zkServers;
//		spoutConfig.zkPort = 2181;
		spoutConfig.forceFromStart = true; // 从头开始消费
		spoutConfig.socketTimeoutMs = 60 * 1000;
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()); // 定义输出为String

		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		// set kafka spout
		builder.setSpout("kafka_spout", kafkaSpout, 3);

		// set bolt
		builder.setBolt("filter", new FilterBolt(), 8).shuffleGrouping("kafka_spout");

		// set kafka bolt
		KafkaBolt kafka_bolt = new KafkaBolt().withTopicSelector(new DefaultTopicSelector("test"))
				.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
		
		builder.setBolt("kafka_bolt", kafka_bolt, 2).shuffleGrouping("filter");

		Config conf = new Config();
		// set producer properties.
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.57.4:9092,192.168.57.5:9092,192.168.57.6:9092");
		props.put("request.required.acks", "1"); // 0  1 -1
		props.put("serializer.class", "kafka.serializer.StringEnscoder");
		conf.put("kafka.broker.properties", props);

//		conf.put(Config.NIMBUS_HOST, "master");
//		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(new String[]{"master","slave1","slave2"}));
//		System.setProperty("storm.jar","D:\\课程\\storm课程\\storm第一天\\课程附件资料\\Storm\\classes\\artifacts\\Storm_jar\\Storm.jar");
//
//		conf.setNumWorkers(4);
//		 StormSubmitter.submitTopologyWithProgressBar("logfilter", conf,
//		 builder.createTopology());
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("logfilter", conf, builder.createTopology());
	}
}
