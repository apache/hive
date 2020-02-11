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
package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.io.BytesWritable;

class Utilities {

  static void testEquality(MapJoinKeyObject key1, MapJoinKeyObject key2) {
    Assert.assertEquals(key1.hashCode(), key2.hashCode());
    Assert.assertEquals(key1, key2);
    Assert.assertEquals(key1.getKeyLength(), key2.getKeyLength());
    Assert.assertTrue(key1.equals(key2));
  }

  static MapJoinKeyObject serde(MapJoinKeyObject key, String columns, String types) 
  throws Exception {
    MapJoinKeyObject result = new MapJoinKeyObject();
    ByteArrayInputStream bais;
    ObjectInputStream in;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    LazyBinarySerDe serde = new LazyBinarySerDe();
    Properties props = new Properties();
    props.put(serdeConstants.LIST_COLUMNS, columns);
    props.put(serdeConstants.LIST_COLUMN_TYPES, types);
    SerDeUtils.initializeSerDe(serde, null, props, null);
    MapJoinObjectSerDeContext context = new MapJoinObjectSerDeContext(serde, false);    
    key.write(context, out);
    out.close();
    bais = new ByteArrayInputStream(baos.toByteArray());
    in = new ObjectInputStream(bais);
    result.read(context, in, new BytesWritable());
    return result;
  }


  static void testEquality(MapJoinRowContainer container1, MapJoinRowContainer container2)
      throws HiveException {
    Assert.assertEquals(container1.rowCount(), container2.rowCount());
    AbstractRowContainer.RowIterator<List<Object>> iter1 = container1.rowIter(),
        iter2 = container2.rowIter();
    for (List<Object> row1 = iter1.first(), row2 = iter2.first();
        row1 != null && row2 != null; row1 = iter1.next(), row2 = iter2.next()) {
      Assert.assertEquals(row1, row2);
    }
  }

  static MapJoinEagerRowContainer serde(
      MapJoinRowContainer container, String columns, String types) throws Exception {
    MapJoinEagerRowContainer result = new MapJoinEagerRowContainer();
    ByteArrayInputStream bais;
    ObjectInputStream in;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    LazyBinarySerDe serde = new LazyBinarySerDe();
    Properties props = new Properties();
    props.put(serdeConstants.LIST_COLUMNS, columns);
    props.put(serdeConstants.LIST_COLUMN_TYPES, types);
    SerDeUtils.initializeSerDe(serde, null, props, null);
    MapJoinObjectSerDeContext context = new MapJoinObjectSerDeContext(serde, true);    
    container.write(context, out);
    out.close();
    bais = new ByteArrayInputStream(baos.toByteArray());
    in = new ObjectInputStream(bais);
    result.read(context, in, new BytesWritable());
    return result;
  }
}
