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
package org.apache.hadoop.hive.metastore.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

//FIXME rename/etc?
@RunWith(Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestScheduledQueries extends MetaStoreClientTest {
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  public TestScheduledQueries(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    client = metaStore.getClient();

  }

  @After
  public void tearDown() throws Exception {
    try {
      client.close();
    } catch (Exception e) {
      // HIVE-19729: Shallow the exceptions based on the discussion in the Jira
    }
    client = null;
  }

  //  @Test
  //  public void test() throws Exception {
  //    List<RuntimeStat> rs0 = client
  //    assertNotNull(rs0);
  //    assertEquals(0, rs0.size());
  //
  //    RuntimeStat stat = createStat(1);
  //    client.addRuntimeStat(stat);
  //
  //    List<RuntimeStat> rs1 = getRuntimeStats();
  //    assertNotNull(rs1);
  //    assertEquals(1, rs1.size());
  //    assertArrayEquals(stat.getPayload(), rs1.get(0).getPayload());
  //    assertEquals(stat.getWeight(), rs1.get(0).getWeight());
  //    // server sets createtime
  //    assertNotEquals(stat.getCreateTime(), rs1.get(0).getCreateTime());
  //
  //    client.addRuntimeStat(createStat(2));
  //    client.addRuntimeStat(createStat(3));
  //    client.addRuntimeStat(createStat(4));
  //
  //    List<RuntimeStat> rs2 = getRuntimeStats();
  //    assertEquals(4, rs2.size());
  //
  //  }

}
