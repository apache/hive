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
import org.apache.hadoop.hive.hbase.HBaseKeyFactory;
import org.apache.hadoop.hive.hbase.HBaseSerDeParameters;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Provides capability to plugin custom implementations for querying of data stored in HBase.
 * */
public interface HBaseValueFactory {

  /**
   * Initialize factory with properties
   * 
   * @param hbaseParam the {@link HBaseParameters hbase parameters}
   * @param conf the hadoop {@link Configuration configuration}
   * @param properties the custom {@link Properties}
   * @throws SerDeException if there was an issue initializing the factory
   */
  void init(HBaseSerDeParameters hbaseParam, Configuration conf, Properties properties)
      throws SerDeException;

  /**
   * create custom object inspector for the value
   * 
   * @param type type information
   * @throws SerDeException if there was an issue creating the {@link ObjectInspector object inspector}
   */
  ObjectInspector createValueObjectInspector(TypeInfo type) throws SerDeException;

  /**
   * create custom object for hbase value
   *
   * @param inspector OI create by {@link HBaseKeyFactory#createKeyObjectInspector}
   */
  LazyObjectBase createValueObject(ObjectInspector inspector) throws SerDeException;

  /**
   * Serialize the given hive object
   * 
   * @param object the object to be serialized
   * @param field the {@link StructField}
   * @return the serialized value
   * @throws {@link IOException} if there was an issue serializing the value
   */
  byte[] serializeValue(Object object, StructField field) throws IOException;
}