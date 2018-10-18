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

import java.io.IOException;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class DefaultHBaseKeyFactory extends AbstractHBaseKeyFactory implements HBaseKeyFactory {

  protected LazySerDeParameters serdeParams;
  protected HBaseRowSerializer serializer;

  @Override
  public void init(HBaseSerDeParameters hbaseParam, Properties properties) throws SerDeException {
    super.init(hbaseParam, properties);
    this.serdeParams = hbaseParam.getSerdeParams();
    this.serializer = new HBaseRowSerializer(hbaseParam);
  }

  @Override
  public ObjectInspector createKeyObjectInspector(TypeInfo type) throws SerDeException {
    return LazyFactory.createLazyObjectInspector(type, 1, serdeParams, ObjectInspectorOptions.JAVA);
  }

  @Override
  public LazyObjectBase createKey(ObjectInspector inspector) throws SerDeException {
    return LazyFactory.createLazyObject(inspector, keyMapping.binaryStorage.get(0));
  }

  @Override
  public byte[] serializeKey(Object object, StructField field) throws IOException {
    return serializer.serializeKeyField(object, field, keyMapping);
  }

  @VisibleForTesting
  static DefaultHBaseKeyFactory forTest(LazySerDeParameters params, ColumnMappings mappings) {
    DefaultHBaseKeyFactory factory = new DefaultHBaseKeyFactory();
    factory.serdeParams = params;
    factory.keyMapping = mappings.getKeyMapping();
    return factory;
  }
}
