/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.server;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Test;
import java.util.Map;
import static org.junit.Assert.assertEquals;

public class TestHiveServer2 {

  @Test
  public void testMaybeStartCompactorThreadsOneCustomPool() {
    HiveServer2 hs2 = new HiveServer2();

    HiveConf conf = new HiveConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 1);
    conf.setInt("hive.compactor.worker.pool1.threads", 1);

    Map<String, Integer> startedWorkers = hs2.maybeStartCompactorThreads(conf);
    assertEquals(1, startedWorkers.size());
    assertEquals(Integer.valueOf(1), startedWorkers.get("pool1"));
  }

  @Test
  public void testMaybeStartCompactorThreadsZeroTotalWorkers() {
    HiveServer2 hs2 = new HiveServer2();

    HiveConf conf = new HiveConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 0);
    conf.setInt("hive.compactor.worker.pool1.threads", 5);

    Map<String, Integer> startedWorkers = hs2.maybeStartCompactorThreads(conf);
    assertEquals(0, startedWorkers.size());
  }

  @Test
  public void testMaybeStartCompactorThreadsZeroCustomWorkers() {
    HiveServer2 hs2 = new HiveServer2();

    HiveConf conf = new HiveConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 5);

    Map<String, Integer> startedWorkers = hs2.maybeStartCompactorThreads(conf);
    assertEquals(1, startedWorkers.size());
    assertEquals(Integer.valueOf(5), startedWorkers.get(Constants.COMPACTION_DEFAULT_POOL));
  }

  @Test
  public void testMaybeStartCompactorThreadsMultipleCustomPools() {
    HiveServer2 hs2 = new HiveServer2();

    HiveConf conf = new HiveConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 12);
    conf.setInt("hive.compactor.worker.pool1.threads", 3);
    conf.setInt("hive.compactor.worker.pool2.threads", 4);
    conf.setInt("hive.compactor.worker.pool3.threads", 5);

    Map<String, Integer> startedWorkers = hs2.maybeStartCompactorThreads(conf);
    assertEquals(3, startedWorkers.size());
    assertEquals(Integer.valueOf(3), startedWorkers.get("pool1"));
    assertEquals(Integer.valueOf(4), startedWorkers.get("pool2"));
    assertEquals(Integer.valueOf(5), startedWorkers.get("pool3"));
  }

  @Test
  public void testMaybeStartCompactorThreadsMultipleCustomPoolsAndDefaultPool() {
    HiveServer2 hs2 = new HiveServer2();

    HiveConf conf = new HiveConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 15);
    conf.setInt("hive.compactor.worker.pool1.threads", 3);
    conf.setInt("hive.compactor.worker.pool2.threads", 4);
    conf.setInt("hive.compactor.worker.pool3.threads", 5);

    Map<String, Integer> startedWorkers = hs2.maybeStartCompactorThreads(conf);
    assertEquals(4, startedWorkers.size());
    assertEquals(Integer.valueOf(3), startedWorkers.get("pool1"));
    assertEquals(Integer.valueOf(4), startedWorkers.get("pool2"));
    assertEquals(Integer.valueOf(5), startedWorkers.get("pool3"));
    assertEquals(Integer.valueOf(3), startedWorkers.get(Constants.COMPACTION_DEFAULT_POOL));
  }
}
