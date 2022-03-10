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
package org.apache.hadoop.hive.hbase;

import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestHBaseStorageHandler {

  @Test
  public void testHbaseConfigIsAddedToJobConf() {
    HBaseStorageHandler hbaseStorageHandler = new HBaseStorageHandler();
    hbaseStorageHandler.setConf(new JobConf(new HiveConf()));

    TableDesc tableDesc = getHBaseTableDesc();

    JobConf jobConfToConfigure = new JobConf(new HiveConf());

    Assert.assertTrue("hbase-site.xml is supposed to be present",
        jobConfToConfigure.get("hbase.some.fake.option.from.xml.file") == null);

    hbaseStorageHandler.configureJobConf(tableDesc, jobConfToConfigure);

    Assert.assertTrue("hbase-site.xml is supposed to be added as a resource by HBaseStorageHandler",
        jobConfToConfigure.get("hbase.some.fake.option.from.xml.file") != null);
  }

  private TableDesc getHBaseTableDesc() {
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties properties = new Properties();
    properties.put(HBaseSerDe.HBASE_COLUMNS_MAPPING, "cf:string");
    properties.put(HBaseSerDe.HBASE_AUTOGENERATE_STRUCT, "true");
    properties.put("cf.string.serialization.type", "avro");
    properties.put("cf.string.serialization.class", "org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization");
    Mockito.when(tableDesc.getProperties()).thenReturn(properties);
    return tableDesc;
  }
}
