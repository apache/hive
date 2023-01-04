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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

@Category(MetastoreCheckinTest.class)
public class TestHiveMetaStoreMethods {

  HMSHandler hmsHandler;
  protected static Warehouse warehouse;
  protected static Configuration conf = null;

  @Before
  public void setUp() throws Exception {
    initConf();
    warehouse = new Warehouse(conf);

    // set some values to use for getting conf. vars
    MetastoreConf.setBoolVar(conf, ConfVars.METRICS_ENABLED, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    hmsHandler = new HMSHandler("hive-conf", conf);
    hmsHandler.init();
  }

  protected void initConf() {
    if (null == conf) {
      conf = MetastoreConf.newMetastoreConf();
    }
  }

  @Test(expected = InvalidObjectException.class)
  public void test_get_partitions_by_names() throws Exception {
    hmsHandler.get_partitions_by_names("dbName", "tblName", Arrays.asList("partNames"));
  }

}
