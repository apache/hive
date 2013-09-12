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
package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.io.BytesWritable;

class Utilities {

  static void testEquality(MapJoinKey key1, MapJoinKey key2) {
    Assert.assertEquals(key1.hashCode(), key2.hashCode());
    Assert.assertEquals(key1, key2);
    Assert.assertEquals(key1.getKey().length, key2.getKey().length);
    int length = key1.getKey().length;
    for (int i = 0; i <length; i++) {
      Assert.assertEquals(key1.getKey()[i], key2.getKey()[i]); 
    }
  }
  
  static MapJoinKey serde(MapJoinKey key, String columns, String types) 
  throws Exception {
    MapJoinKey result = new MapJoinKey();
    ByteArrayInputStream bais;
    ObjectInputStream in;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    LazyBinarySerDe serde = new LazyBinarySerDe();
    Properties props = new Properties();
    props.put(serdeConstants.LIST_COLUMNS, columns);
    props.put(serdeConstants.LIST_COLUMN_TYPES, types);
    serde.initialize(null, props);
    MapJoinObjectSerDeContext context = new MapJoinObjectSerDeContext(serde, false);    
    key.write(context, out);
    out.close();
    bais = new ByteArrayInputStream(baos.toByteArray());
    in = new ObjectInputStream(bais);
    result.read(context, in, new BytesWritable());
    return result;
  }
  
  
  static void testEquality(MapJoinRowContainer container1, MapJoinRowContainer container2) {
    Assert.assertEquals(container1.size(), container2.size());
    List<Object> row1 = container1.first();
    List<Object> row2 = container2.first();
    for (; row1 != null && row2 != null; row1 = container1.next(), row2 = container2.next()) {
      Assert.assertEquals(row1, row2);
    }
  }
  
  static MapJoinRowContainer serde(MapJoinRowContainer container, String columns, String types) 
  throws Exception {
    MapJoinRowContainer result = new MapJoinRowContainer();
    ByteArrayInputStream bais;
    ObjectInputStream in;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    LazyBinarySerDe serde = new LazyBinarySerDe();
    Properties props = new Properties();
    props.put(serdeConstants.LIST_COLUMNS, columns);
    props.put(serdeConstants.LIST_COLUMN_TYPES, types);
    serde.initialize(null, props);
    MapJoinObjectSerDeContext context = new MapJoinObjectSerDeContext(serde, true);    
    container.write(context, out);
    out.close();
    bais = new ByteArrayInputStream(baos.toByteArray());
    in = new ObjectInputStream(bais);
    result.read(context, in, new BytesWritable());
    return result;
  }
}
