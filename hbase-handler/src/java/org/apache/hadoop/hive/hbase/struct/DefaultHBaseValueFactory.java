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
package org.apache.hadoop.hive.hbase.struct;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.hbase.ColumnMappings;
import org.apache.hadoop.hive.hbase.HBaseSerDeHelper;
import org.apache.hadoop.hive.hbase.HBaseSerDeParameters;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Default implementation of the {@link HBaseValueFactory}
 * */
public class DefaultHBaseValueFactory implements HBaseValueFactory {

  protected LazySerDeParameters serdeParams;
  protected ColumnMappings columnMappings;
  protected HBaseSerDeParameters hbaseParams;
  protected Properties properties;
  protected Configuration conf;

  private int fieldID;

  public DefaultHBaseValueFactory(int fieldID) {
    this.fieldID = fieldID;
  }

  @Override
  public void init(HBaseSerDeParameters hbaseParams, Configuration conf, Properties properties)
      throws SerDeException {
    this.hbaseParams = hbaseParams;
    this.serdeParams = hbaseParams.getSerdeParams();
    this.columnMappings = hbaseParams.getColumnMappings();
    this.properties = properties;
    this.conf = conf;
  }

  @Override
  public ObjectInspector createValueObjectInspector(TypeInfo type)
      throws SerDeException {
    return LazyFactory.createLazyObjectInspector(type,
        1, serdeParams, ObjectInspectorOptions.JAVA);
  }

  @Override
  public LazyObjectBase createValueObject(ObjectInspector inspector) throws SerDeException {
    return HBaseSerDeHelper.createLazyField(columnMappings.getColumnsMapping(), fieldID, inspector);
  }

  @Override
  public byte[] serializeValue(Object object, StructField field)
      throws IOException {
    // TODO Add support for serialization of values here
    return null;
  }
}
