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

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.avro.AvroSchemaRetriever;

/**
 * Mock implementation
 * */
public class HBaseTestAvroSchemaRetriever extends AvroSchemaRetriever {

  private static final byte[] TEST_BYTE_ARRAY = Bytes.toBytes("test");

  public HBaseTestAvroSchemaRetriever(Configuration conf, Properties tbl) {
  }

  @Override
  public Schema retrieveWriterSchema(Object source) {
    Class<?> clazz;
    try {
      clazz = Class.forName("org.apache.hadoop.hive.hbase.avro.Employee");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    return ReflectData.get().getSchema(clazz);
  }

  @Override
  public Schema retrieveReaderSchema(Object source) {
    Class<?> clazz;
    try {
      clazz = Class.forName("org.apache.hadoop.hive.hbase.avro.Employee");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    return ReflectData.get().getSchema(clazz);
  }

  @Override
  public int getOffset() {
    return TEST_BYTE_ARRAY.length;
  }
}