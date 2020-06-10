/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.serde;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Interface for providing custom Accumulo RowID generation/parsing
 */
public interface AccumuloRowIdFactory {

  /**
   * initialize factory with properties
   */
  public void init(AccumuloSerDeParameters serDeParams, Properties properties)
      throws SerDeException;

  /**
   * create custom object inspector for accumulo rowId
   *
   * @param type
   *          type information
   */
  public ObjectInspector createRowIdObjectInspector(TypeInfo type) throws SerDeException;

  /**
   * create custom object for accumulo
   *
   * @param inspector
   *          OI create by {@link AccumuloRowIdFactory#createRowIdObjectInspector}
   */
  public LazyObjectBase createRowId(ObjectInspector inspector) throws SerDeException;

  /**
   * serialize hive object in internal format of custom key
   */
  public byte[] serializeRowId(Object object, StructField field, ByteStream.Output output)
      throws IOException;

  /**
   * Add this implementation to the classpath for the Job
   */
  public void addDependencyJars(Configuration conf) throws IOException;
}
