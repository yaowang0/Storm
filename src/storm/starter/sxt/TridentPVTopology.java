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
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.kafka.*;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class TridentPVTopology {

  public static class ParseLog extends BaseFunction {
	private static final long serialVersionUID = 1L;
	private static SimpleDateFormat format = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss", Locale.US);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      // log format:
      // 205.212.115.106 - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/countdown.html HTTP/1.0" 200 3985
      String line = tuple.getString(0);
      System.out.println(line);
      String[] splits = line.split(" ");
      if (splits.length > 6) {
        String time = splits[3];
        String url = splits[6];
//        System.out.println(time);
//        System.out.println(url);
        int index = url.indexOf("?");
        if (index > 0) {
          url = url.substring(0, index);
        }
        try {
          format.parse(time).getTime();
          collector.emit(new Values(time.substring(1, time.length() - 3) + ":00_" + url));
        } catch (Exception e) {
          System.err.println("can not parse time for log " + line);
        }
      } else {
        System.err.println("can not parse for log " + line);
      }
    }
  }

  public static class ParseArg extends BaseFunction {
	private static final long serialVersionUID = 1L;

	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String[] splits = tuple.getString(0).split(" ", 2);
      if (splits.length == 2) {
        collector.emit(new Values(splits[0], splits[1]));
      }
    }
  }

  public static void main(String[] args) {
	  // config kafka spout
	  String topicName = "test";
    String topoName = topicName + "_tridentpv";

    // create TransactionalTridentKafkaSpout
    ZkHosts zkHosts = new ZkHosts("192.168.80.201:2181,192.168.80.202:2181,192.168.80.203:2181");
    TridentKafkaConfig tridentKafkaConfig =
            new TridentKafkaConfig(zkHosts, topicName, UUID.randomUUID().toString());
    List<String> zkServers = new ArrayList<String>();
	System.out.println(zkHosts.brokerZkStr);
	for (String host : zkHosts.brokerZkStr.split(",")) {
		zkServers.add(host.split(":")[0]);
	}

    tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    tridentKafkaConfig.forceFromStart = true;
    TransactionalTridentKafkaSpout tridentKafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);

    // create transactional RedisState
    StateFactory state = RedisState.transactional(new InetSocketAddress("192.168.80.201", 6379), topoName);

    // compute pv and storm it to redis
    TridentTopology topology = new TridentTopology();
//    TridentState pvState = 
    		topology.newStream(topicName, tridentKafkaSpout)
            .each(new Fields("str"), new ParseLog(), new Fields("time_url"))
            .groupBy(new Fields("time_url"))
            .persistentAggregate(state, new Count(), new Fields("pv"));

    // query pv from pvState by DRPC
//    topology.newDRPCStream(topoName + "_query")
//            .stateQuery(pvState, new Fields("args"), new MapGet(), new Fields("pv"));

    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(3);
//    StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topology.build());
    LocalCluster localCluster = new LocalCluster();
	localCluster.submitTopology(topicName + "_pv", conf, topology.build());
  }
}
