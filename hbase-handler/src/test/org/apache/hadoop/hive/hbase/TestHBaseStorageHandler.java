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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
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

  @Test
  public void testGetUriForAuth() {
    try {
      HBaseStorageHandler hbaseStorageHandler = new HBaseStorageHandler();
      hbaseStorageHandler.setConf(new JobConf(new HiveConf()));
      Table table = createMockTable(new HashMap<>());
      URI uri = hbaseStorageHandler.getURIForAuth(table);
      // If there is no tablename provided, the default "null" is still
      // written out. At the time this test was written, this was the current
      // behavior, so I left this test as/is. Need to research if a null
      // table can be provided here.
      Assert.assertEquals("hbase://localhost:2181/null", uri.toString());

      Map<String, String> serdeParams = new HashMap<>();
      serdeParams.put("hbase.zookeeper.quorum", "testhost");
      serdeParams.put("hbase.zookeeper.property.clientPort", "8765");
      table = createMockTable(serdeParams);
      uri = hbaseStorageHandler.getURIForAuth(table);
      Assert.assertEquals("hbase://testhost:8765/null", uri.toString());

      serdeParams.put("hbase.table.name", "mytbl");
      table = createMockTable(serdeParams);
      uri = hbaseStorageHandler.getURIForAuth(table);
      Assert.assertEquals("hbase://testhost:8765/mytbl", uri.toString());

      serdeParams.put("hbase.columns.mapping", "mycolumns");
      table = createMockTable(serdeParams);
      uri = hbaseStorageHandler.getURIForAuth(table);
      Assert.assertEquals("hbase://testhost:8765/mytbl/mycolumns", uri.toString());

      serdeParams.put("hbase.table.name", "my#tbl");
      serdeParams.put("hbase.columns.mapping", "myco#lumns");
      table = createMockTable(serdeParams);
      uri = hbaseStorageHandler.getURIForAuth(table);
      Assert.assertEquals("hbase://testhost:8765/my%23tbl/myco%23lumns", uri.toString());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
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

  private Table createMockTable(Map<String, String> serdeParams) {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    SerDeInfo sdi = new SerDeInfo();
    Map<String, String> params = new HashMap<>();
    sdi.setParameters(serdeParams);
    sd.setSerdeInfo(sdi);
    table.setSd(sd);
    table.setParameters(params);
    return table;
  }
}
