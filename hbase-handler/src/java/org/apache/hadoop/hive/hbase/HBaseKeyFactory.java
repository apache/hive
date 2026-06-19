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

import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Properties;

/**
 * Provides custom implementation of object and object inspector for hbase key.
 * The key object should implement LazyObjectBase.
 *
 * User can optionally implement HiveStoragePredicateHandler for handling filter predicates
 */
public interface HBaseKeyFactory extends HiveStoragePredicateHandler {

  /**
   * initialize factory with properties
   */
  void init(HBaseSerDeParameters hbaseParam, Properties properties) throws SerDeException;

  /**
   * create custom object inspector for hbase key
   * @param type type information
   */
  ObjectInspector createKeyObjectInspector(TypeInfo type) throws SerDeException;

  /**
   * create custom object for hbase key
   * @param inspector OI create by {@link HBaseKeyFactory#createKeyObjectInspector}
   */
  LazyObjectBase createKey(ObjectInspector inspector) throws SerDeException;

  /**
   * serialize hive object in internal format of custom key
   *
   * @param object
   * @param field
   *
   * @return true if it's not null
   * @throws java.io.IOException
   */
  byte[] serializeKey(Object object, StructField field) throws IOException;

  /**
   * configure jobConf for this factory
   *
   * @param tableDesc
   * @param jobConf
   */
  void configureJobConf(TableDesc tableDesc, JobConf jobConf) throws IOException;
}
