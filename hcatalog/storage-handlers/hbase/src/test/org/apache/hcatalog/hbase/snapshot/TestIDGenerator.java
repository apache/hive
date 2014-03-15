/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.hbase.snapshot;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hcatalog.hbase.SkeletonHBaseTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestIDGenerator extends SkeletonHBaseTest {

  @BeforeClass
  public static void setup() throws Throwable {
    setupSkeletonHBaseTest();
  }

  @Test
  public void testIDGeneration() throws Exception {

    int port = getHbaseConf().getInt("hbase.zookeeper.property.clientPort", 2181);
    String servers = getHbaseConf().get("hbase.zookeeper.quorum");
    String[] splits = servers.split(",");
    StringBuffer sb = new StringBuffer();
    for (String split : splits) {
      sb.append(split);
      sb.append(':');
      sb.append(port);
    }
    ZKUtil zkutil = new ZKUtil(sb.toString(), "/rm_base");

    String tableName = "myTable";
    long initId = zkutil.nextId(tableName);
    for (int i = 0; i < 10; i++) {
      long id = zkutil.nextId(tableName);
      Assert.assertEquals(initId + (i + 1), id);
    }
  }

  @Test
  public void testMultipleClients() throws InterruptedException {

    int port = getHbaseConf().getInt("hbase.zookeeper.property.clientPort", 2181);
    String servers = getHbaseConf().get("hbase.zookeeper.quorum");
    String[] splits = servers.split(",");
    StringBuffer sb = new StringBuffer();
    for (String split : splits) {
      sb.append(split);
      sb.append(':');
      sb.append(port);
    }

    ArrayList<IDGenClient> clients = new ArrayList<IDGenClient>();

    for (int i = 0; i < 5; i++) {
      IDGenClient idClient = new IDGenClient(sb.toString(), "/rm_base", 10, "testTable");
      clients.add(idClient);
    }

    for (IDGenClient idClient : clients) {
      idClient.run();
    }

    for (IDGenClient idClient : clients) {
      idClient.join();
    }

    HashMap<Long, Long> idMap = new HashMap<Long, Long>();
    for (IDGenClient idClient : clients) {
      idMap.putAll(idClient.getIdMap());
    }

    ArrayList<Long> keys = new ArrayList<Long>(idMap.keySet());
    Collections.sort(keys);
    int startId = 1;
    for (Long key : keys) {
      Long id = idMap.get(key);
      System.out.println("Key: " + key + " Value " + id);
      assertTrue(id == startId);
      startId++;

    }
  }
}
