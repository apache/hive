/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.arrow;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestSerializer {
  @Test
  public void testEmptyList() {
    List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString("array<tinyint>");
    List<String> fieldNames = Arrays.asList(new String[]{"a"});
    Serializer converter = new Serializer(new HiveConf(), "attemptId", typeInfos, fieldNames);
    ArrowWrapperWritable writable = converter.emptyBatch();
    Assert.assertEquals("Schema<a: List<$data$: Int(8, true)>>",
        writable.getVectorSchemaRoot().getSchema().toString());
  }

  @Test
  public void testEmptyStruct() {
    List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString("struct<b:int,c:string>");
    List<String> fieldNames = Arrays.asList(new String[] { "a" });
    Serializer converter = new Serializer(new HiveConf(), "attemptId", typeInfos, fieldNames);
    ArrowWrapperWritable writable = converter.emptyBatch();
    Assert.assertEquals("Schema<a: Struct<b: Int(32, true), c: Utf8>>",
        writable.getVectorSchemaRoot().getSchema().toString());
  }

  @Test
  public void testEmptyMap() {
    List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString("map<string,string>");
    List<String> fieldNames = Arrays.asList(new String[] { "a" });
    Serializer converter = new Serializer(new HiveConf(), "attemptId", typeInfos, fieldNames);
    ArrowWrapperWritable writable = converter.emptyBatch();
    Assert.assertEquals("Schema<a: Map(false)<entries: Struct<key: Utf8 not null, value: Utf8 not null> not null>>",
        writable.getVectorSchemaRoot().getSchema().toString());
  }

  @Test
  public void testEmptyComplexStruct() {
    List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(
        "struct<b:array<tinyint>,c:map<string,string>,d:struct<e:array<tinyint>,f:map<string,string>>>");
    List<String> fieldNames = Arrays.asList(new String[] { "a" });
    Serializer converter = new Serializer(new HiveConf(), "attemptId", typeInfos, fieldNames);
    ArrowWrapperWritable writable = converter.emptyBatch();
    Assert.assertEquals(
        "Schema<a: Struct<b: List<$data$: Int(8, true)>, c: Map(false)<entries: Struct<key: Utf8 not null, " +
                "value: Utf8 not null> not null>, d: Struct<e: List<$data$: Int(8, true)>, f: Map(false)<entries: " +
                "Struct<key: Utf8 not null, value: Utf8 not null> not null>>>>",
        writable.getVectorSchemaRoot().getSchema().toString());
  }

  @Test
  public void testEmptyComplexMap() {
    List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(
        "map<array<tinyint>,struct<b:array<tinyint>,c:map<string,string>>>");
    List<String> fieldNames = Arrays.asList(new String[] { "a" });
    Serializer converter = new Serializer(new HiveConf(), "attemptId", typeInfos, fieldNames);
    ArrowWrapperWritable writable = converter.emptyBatch();
    Assert.assertEquals(
        "Schema<a: Map(false)<entries: Struct<key: List<$data$: Int(8, true)> not null, value: Struct<b: " +
                "List<$data$: Int(8, true)>, c: Map(false)<entries: Struct<key: Utf8 not null, value: Utf8 not null> " +
                "not null>> not null> not null>>",
        writable.getVectorSchemaRoot().getSchema().toString());
  }

  @Test
  public void testEmptyComplexList() {
    List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString("struct<b:array<array<tinyint>>," +
        "c:array<map<string,string>>,d:array<struct<e:array<tinyint>,f:map<string,string>>>>");
    List<String> fieldNames = Arrays.asList(new String[] { "a" });
    Serializer converter = new Serializer(new HiveConf(), "attemptId", typeInfos, fieldNames);
    ArrowWrapperWritable writable = converter.emptyBatch();
    Assert.assertEquals(
        "Schema<a: Struct<b: List<$data$: List<$data$: Int(8, true)>>, c: List<$data$: Map(false)<entries: " +
                "Struct<key: Utf8 not null, value: Utf8 not null> not null>>, d: List<$data$: Struct<e: List<$data$: " +
                "Int(8, true)>, f: Map(false)<entries: Struct<key: Utf8 not null, value: Utf8 not null> not null>>>>>",
        writable.getVectorSchemaRoot().getSchema().toString());
  }
}
