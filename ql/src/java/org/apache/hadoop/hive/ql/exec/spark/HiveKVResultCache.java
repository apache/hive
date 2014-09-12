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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Reporter;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper around {@link org.apache.hadoop.hive.ql.exec.persistence.RowContainer}
 */
public class HiveKVResultCache {

  public static final int IN_MEMORY_CACHE_SIZE = 512;
  private static final String COL_NAMES = "key,value";
  private static final String COL_TYPES =
      serdeConstants.BINARY_TYPE_NAME + ":" + serdeConstants.BINARY_TYPE_NAME;

  private RowContainer container;
  private int cursor = 0;

  public HiveKVResultCache(Configuration conf) {
    initRowContainer(conf);
  }

  private void initRowContainer(Configuration conf) {
    try {
      container = new RowContainer(IN_MEMORY_CACHE_SIZE, conf, Reporter.NULL);

      String fileFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);
      TableDesc tableDesc =
          PlanUtils.getDefaultQueryOutputTableDesc(COL_NAMES, COL_TYPES, fileFormat);

      SerDe serDe = (SerDe) tableDesc.getDeserializer();
      ObjectInspector oi = ObjectInspectorUtils.getStandardObjectInspector(
          serDe.getObjectInspector(), ObjectInspectorCopyOption.WRITABLE);

      container.setSerDe(serDe, oi);
      container.setTableDesc(tableDesc);
    } catch(Exception ex) {
      throw new RuntimeException("Failed to create RowContainer", ex);
    }
  }

  public void add(HiveKey key, BytesWritable value) {
    byte[] hiveKeyBytes = KryoSerializer.serialize(key);
    BytesWritable wrappedHiveKey = new BytesWritable(hiveKeyBytes);
    List<BytesWritable> row = new ArrayList<BytesWritable>(2);
    row.add(wrappedHiveKey);
    row.add(value);

    try {
      container.addRow(row);
    } catch (HiveException ex) {
      throw new RuntimeException("Failed to add KV pair to RowContainer", ex);
    }
  }

  public void clear() {
    try {
      container.clearRows();
    } catch(HiveException ex) {
      throw new RuntimeException("Failed to clear rows in RowContainer", ex);
    }
    cursor = 0;
  }

  public boolean hasNext() {
    return container.rowCount() > 0 && cursor < container.rowCount();
  }

  public Tuple2<HiveKey, BytesWritable> next() {
    Preconditions.checkState(hasNext());

    try {
      List<BytesWritable> row;
      if (cursor == 0) {
        row = container.first();
      } else {
        row = container.next();
      }
      cursor++;
      HiveKey key = KryoSerializer.deserialize(row.get(0).getBytes(), HiveKey.class);
      return new Tuple2<HiveKey, BytesWritable>(key, row.get(1));
    } catch (HiveException ex) {
      throw new RuntimeException("Failed to get row from RowContainer", ex);
    }
  }
}
