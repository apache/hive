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

import java.util.ArrayList;
import java.util.List;

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

import com.google.common.base.Preconditions;

/**
 * Wrapper around {@link org.apache.hadoop.hive.ql.exec.persistence.RowContainer}
 *
 * This class is thread safe.
 */
@SuppressWarnings({"deprecation", "unchecked", "rawtypes"})
public class HiveKVResultCache {

  public static final int IN_MEMORY_CACHE_SIZE = 512;
  private static final String COL_NAMES = "key,value";
  private static final String COL_TYPES =
      serdeConstants.BINARY_TYPE_NAME + ":" + serdeConstants.BINARY_TYPE_NAME;

  // Used to cache rows added while container is iterated.
  private RowContainer backupContainer;

  private RowContainer container;
  private Configuration conf;
  private int cursor = 0;

  public HiveKVResultCache(Configuration conf) {
    container = initRowContainer(conf);
    this.conf = conf;
  }

  private static RowContainer initRowContainer(Configuration conf) {
    RowContainer container;
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
    } catch (Exception ex) {
      throw new RuntimeException("Failed to create RowContainer", ex);
    }
    return container;
  }

  public void add(HiveKey key, BytesWritable value) {
    byte[] hiveKeyBytes = KryoSerializer.serialize(key);
    BytesWritable wrappedHiveKey = new BytesWritable(hiveKeyBytes);
    List<BytesWritable> row = new ArrayList<BytesWritable>(2);
    row.add(wrappedHiveKey);
    row.add(value);

    synchronized (this) {
      try {
        if (cursor == 0) {
          container.addRow(row);
        } else {
          if (backupContainer == null) {
            backupContainer = initRowContainer(conf);
          }
          backupContainer.addRow(row);
        }
      } catch (HiveException ex) {
        throw new RuntimeException("Failed to add KV pair to RowContainer", ex);
      }
    }
  }

  public synchronized void clear() {
    if (cursor == 0) {
      return;
    }
    try {
      container.clearRows();
    } catch (HiveException ex) {
      throw new RuntimeException("Failed to clear rows in RowContainer", ex);
    }
    cursor = 0;
  }

  public synchronized boolean hasNext() {
    if (container.rowCount() > 0 && cursor < container.rowCount()) {
      return true;
    }
    if (backupContainer == null
        || backupContainer.rowCount() == 0) {
      return false;
    }
    clear();
    // Switch containers
    RowContainer tmp = container;
    container = backupContainer;
    backupContainer = tmp;
    return true;
  }

  public Tuple2<HiveKey, BytesWritable> next() {
    try {
      List<BytesWritable> row;
      synchronized (this) {
        Preconditions.checkState(hasNext());
        if (cursor == 0) {
          row = container.first();
        } else {
          row = container.next();
        }
        cursor++;
      }
      HiveKey key = KryoSerializer.deserialize(row.get(0).getBytes(), HiveKey.class);
      return new Tuple2<HiveKey, BytesWritable>(key, row.get(1));
    } catch (HiveException ex) {
      throw new RuntimeException("Failed to get row from RowContainer", ex);
    }
  }
}
