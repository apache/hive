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
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.List;

public class HBaseStoreTestUtil {
  public static void initHBaseMetastore(HBaseAdmin admin, HiveConf conf) throws Exception {
    for (String tableName : HBaseReadWrite.tableNames) {
      List<byte[]> families = HBaseReadWrite.columnFamilies.get(tableName);
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
      for (byte[] family : families) {
        HColumnDescriptor columnDesc = new HColumnDescriptor(family);
        desc.addFamily(columnDesc);
      }
      admin.createTable(desc);
    }
    admin.close();
    if (conf != null) {
      HBaseReadWrite.setConf(conf);
    }
  }
}