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
package org.apache.hadoop.hive.hbase.struct;

import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Implementation of {@link HBaseValueFactory} to consume a custom struct
 * */
public class StructHBaseValueFactory<T extends HBaseStructValue> extends DefaultHBaseValueFactory {

  private final int fieldID;
  private final Constructor constructor;

  public StructHBaseValueFactory(int fieldID, Class<?> structValueClass) throws Exception {
    super(fieldID);
    this.fieldID = fieldID;
    this.constructor =
        structValueClass.getDeclaredConstructor(LazySimpleStructObjectInspector.class,
            Properties.class, Configuration.class, ColumnMapping.class);
  }

  @Override
  public LazyObjectBase createValueObject(ObjectInspector inspector) throws SerDeException {
    try {
      return (T) constructor.newInstance(inspector, properties, hbaseParams.getBaseConfiguration(),
          hbaseParams.getColumnMappings().getColumnsMapping()[fieldID]);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }
}