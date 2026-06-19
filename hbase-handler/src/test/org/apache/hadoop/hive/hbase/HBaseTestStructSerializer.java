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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.hbase.struct.HBaseStructValue;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Test specific implementation of {@link org.apache.hadoop.hive.serde2.lazy.LazyStruct}
 */
public class HBaseTestStructSerializer extends HBaseStructValue {

  protected byte[] bytes;
  protected String bytesAsString;
  protected Properties tbl;
  protected Configuration conf;
  protected ColumnMapping colMapping;
  protected String testValue;

  public HBaseTestStructSerializer(LazySimpleStructObjectInspector oi, Properties tbl,
      Configuration conf, ColumnMapping colMapping) {
    super(oi);
    this.tbl = tbl;
    this.conf = conf;
    this.colMapping = colMapping;
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    this.bytes = bytes.getData();
  }

  @Override
  public Object getField(int fieldID) {
    if (bytesAsString == null) {
      bytesAsString = Bytes.toString(bytes).trim();
    }

    // Randomly pick the character corresponding to the field id and convert it to byte array
    byte[] fieldBytes = new byte[] { (byte) bytesAsString.charAt(fieldID) };

    return toLazyObject(fieldID, fieldBytes);
  }

  /**
   * Create an initialize a {@link LazyObject} with the given bytes for the given fieldID.
   *
   * @param fieldID field for which the object is to be created
   * @param bytes value with which the object is to be initialized with
   * 
   * @return initialized {@link LazyObject}
   * */
  @Override
  public LazyObject<? extends ObjectInspector> toLazyObject(int fieldID, byte[] bytes) {
    ObjectInspector fieldOI = oi.getAllStructFieldRefs().get(fieldID).getFieldObjectInspector();

    LazyObject<? extends ObjectInspector> lazyObject = LazyFactory.createLazyObject(fieldOI);

    ByteArrayRef ref = new ByteArrayRef();

    ref.setData(bytes);

    // initialize the lazy object
    lazyObject.init(ref, 0, ref.getData().length);

    return lazyObject;
  }
}
