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


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class KafkaUtil {
  public static void main(String[] args) throws Exception {
    ProducerConfig config = new ProducerConfig(System.getProperties());
    Producer<String, String> producer = new Producer<String, String>(config);

    String topic = args[0];

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    StringBuffer sb = new StringBuffer();
    int n = 0;
    String line = null;
    while ((line = reader.readLine()) != null) {
      if (n != 0) sb.append("\n");
      sb.append(line);
      n++;

      if (n == 100) {
        producer.send(new KeyedMessage<String, String>(topic, null, sb.toString()));
        System.out.println("sent " + n + " lines top kafka topic " + topic);
        sb.delete(0, sb.length());
        n = 0;
      }
    }
    if (n > 0) {
      producer.send(new KeyedMessage<String, String>(topic, null, sb.toString()));
      System.out.println("sent " + n + " lines top kafka topic " + topic);
    }
  }
}

