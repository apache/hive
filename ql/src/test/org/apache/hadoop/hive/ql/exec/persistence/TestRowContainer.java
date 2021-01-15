/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.persistence;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestRowContainer {

  @Test
  public void testSpillTimestamp() throws HiveException, SerDeException, IOException {
    int blockSize = 10;
    Configuration cfg = new Configuration();
    RowContainer result = new RowContainer(blockSize, cfg, null);
    LazyBinarySerDe serde = new LazyBinarySerDe();
    Properties props = new Properties();
    props.put(serdeConstants.LIST_COLUMNS, "x");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "array<string>");
    serde.initialize(null, props, null);
    result.setSerDe(serde,
      ObjectInspectorUtils.getStandardObjectInspector(serde.getObjectInspector()));
    result.setTableDesc(
      PTFRowContainer.createTableDesc((StructObjectInspector) serde.getObjectInspector()));
    TimestampWritableV2 key = new TimestampWritableV2(Timestamp.ofEpochMilli(10));
    result.setKeyObject(Lists.newArrayList(key));
    List<Writable> row;
    // will trigger 2 spills
    for (int i = 0; i <= blockSize * 2; i++) {
      row = new ArrayList<Writable>();
      row.add(new Text("" + i));
      result.addRow(row);
    }
    assertEquals(2, result.getNumFlushedBlocks());
    result.setKeyObject(null);
    assertEquals(Lists.newArrayList(0).toString(), result.first().get(0).toString());
    for (int i = 1; i < result.rowCount() - 1; i++) {
      assertEquals(Lists.newArrayList(i).toString(), result.next().get(0).toString());
    }
    result.close();
  }
}
