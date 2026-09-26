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

package org.apache.hive.search.metastore;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(MetastoreUnitTest.class)
public class TestTableBlobCodec {

  @Test
  public void roundTripPreservesTableFields() throws Exception {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("sales");
    table.setTableName("orders");
    table.setOwner("alice");
    table.setSd(new StorageDescriptor());
    table.getSd().setLocation("hdfs://warehouse/orders");
    Map<String, String> params = new HashMap<>();
    params.put("comment", "daily orders");
    table.setParameters(params);

    byte[] encoded = TableBlobCodec.encode(table);
    Table decoded = TableBlobCodec.decode(encoded);

    assertEquals("hive", decoded.getCatName());
    assertEquals("sales", decoded.getDbName());
    assertEquals("orders", decoded.getTableName());
    assertEquals("alice", decoded.getOwner());
    assertEquals("hdfs://warehouse/orders", decoded.getSd().getLocation());
    assertEquals("daily orders", decoded.getParameters().get("comment"));
  }

  @Test
  public void encodeIsDeterministicForSameTable() throws Exception {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("default");
    table.setTableName("t");

    assertArrayEquals(TableBlobCodec.encode(table), TableBlobCodec.encode(table));
  }

  @Test
  public void gzipShrinksRepetitivePayload() throws Exception {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("default");
    table.setTableName("wide");
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(List.of(
        new org.apache.hadoop.hive.metastore.api.FieldSchema("c1", "string", "x".repeat(200)),
        new org.apache.hadoop.hive.metastore.api.FieldSchema("c2", "string", "y".repeat(200))));

    byte[] encoded = TableBlobCodec.encode(table);
    assertFalse(encoded.length == 0);
  }

  @Test
  public void excludePatternsDropMatchingTableParameters() throws Exception {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("sales");
    table.setTableName("orders");
    Map<String, String> params = new HashMap<>();
    params.put("comment", "keep me");
    params.put("spark.sql.sources.schema", "huge-schema-payload");
    table.setParameters(params);

    Table decoded = TableBlobCodec.decode(
        TableBlobCodec.encode(table, List.of("spark\\.sql\\.sources\\..*")));

    assertEquals("keep me", decoded.getParameters().get("comment"));
    assertFalse(decoded.getParameters().containsKey("spark.sql.sources.schema"));
  }

  @Test
  public void excludePatternsDropMatchingStorageDescriptorParameters() throws Exception {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("default");
    table.setTableName("t");
    table.setSd(new StorageDescriptor());
    table.getSd().setParameters(Map.of("keep", "yes", "spark.statistics", "big"));

    Table decoded = TableBlobCodec.decode(
        TableBlobCodec.encode(table, List.of("^spark\\..*")));

    assertEquals("yes", decoded.getSd().getParameters().get("keep"));
    assertFalse(decoded.getSd().getParameters().containsKey("spark.statistics"));
  }

  @Test
  public void excludePatternsDoNotMutateSourceTable() throws Exception {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("default");
    table.setTableName("t");
    table.setParameters(new HashMap<>(Map.of("drop.me", "value")));

    TableBlobCodec.encode(table, List.of("drop\\.me"));
    assertEquals("value", table.getParameters().get("drop.me"));
  }
}
