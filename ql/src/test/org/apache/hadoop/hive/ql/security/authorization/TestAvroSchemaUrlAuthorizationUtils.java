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
package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestAvroSchemaUrlAuthorizationUtils {

  @Test
  public void getFilesystemSchemaUrlToAuthorizeRequiresAvroSerde() {
    org.apache.hadoop.hive.metastore.api.Table table = avroTable("hdfs://nn/schema.avsc", null);
    table.getSd().getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    assertNull(AuthorizationUtils.getFilesystemAvroSchemaUrlToAuthorize(new Table(table)));
  }

  @Test
  public void getFilesystemSchemaUrlToAuthorizeSkipsWhenLiteralPresent() {
    org.apache.hadoop.hive.metastore.api.Table table = avroTable("hdfs://nn/schema.avsc",
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":[]}");
    assertNull(AuthorizationUtils.getFilesystemAvroSchemaUrlToAuthorize(new Table(table)));
  }

  @Test
  public void getFilesystemSchemaUrlToAuthorizeReturnsHdfsUrl() {
    String schemaUrl = "hdfs://nn/schema.avsc";
    org.apache.hadoop.hive.metastore.api.Table table = avroTable(schemaUrl, null);
    assertEquals(schemaUrl,
        AuthorizationUtils.getFilesystemAvroSchemaUrlToAuthorize(new Table(table)));
  }

  @Test
  public void getFilesystemSchemaUrlToAuthorizeRejectsHttpUrl() {
    org.apache.hadoop.hive.metastore.api.Table table = avroTable("http://example.com/schema.avsc", null);
    assertNull(AuthorizationUtils.getFilesystemAvroSchemaUrlToAuthorize(new Table(table)));
  }

  @Test
  public void addSchemaUrlInputForReadEntityAddsDfsReadEntity() {
    String schemaUrl = "hdfs://nn/schema.avsc";
    Table table = new Table(avroTable(schemaUrl, null));
    Set<ReadEntity> inputs = new LinkedHashSet<>();
    ReadEntity tableInput = new ReadEntity(table);
    inputs.add(tableInput);

    AuthorizationUtils.addAvroSchemaUrlInputForReadEntity(inputs, tableInput);

    assertEquals(2, inputs.size());
    assertTrue(inputs.stream().anyMatch(input -> input.getTyp() == Entity.Type.DFS_DIR
        && input.getD().toString().contains(schemaUrl)));
  }

  @Test
  public void addSchemaUrlInputForReadEntitySkipsIndirectReads() {
    Table table = new Table(avroTable("hdfs://nn/schema.avsc", null));
    Set<ReadEntity> inputs = new LinkedHashSet<>();
    ReadEntity tableInput = new ReadEntity(table, null, false);
    inputs.add(tableInput);

    AuthorizationUtils.addAvroSchemaUrlInputForReadEntity(inputs, tableInput);

    assertEquals(1, inputs.size());
  }

  @Test
  public void addSchemaUrlInputForReadEntityAddsDfsDirEntity() {
    String schemaUrl = "hdfs://nn/schema.avsc";
    Table table = new Table(avroTable(schemaUrl, null));
    Set<ReadEntity> inputs = new LinkedHashSet<>();
    ReadEntity tableInput = new ReadEntity(table);
    inputs.add(tableInput);

    AuthorizationUtils.addAvroSchemaUrlInputForReadEntity(inputs, tableInput);

    boolean foundDfsInput = false;
    for (ReadEntity input : inputs) {
      if (input.getTyp() == Entity.Type.DFS_DIR) {
        foundDfsInput = true;
        assertTrue(input.getD().toString().contains("hdfs://nn/schema.avsc"));
      }
    }
    assertTrue(foundDfsInput);
  }

  private static org.apache.hadoop.hive.metastore.api.Table avroTable(String schemaUrl,
      String schemaLiteral) {
    org.apache.hadoop.hive.metastore.api.Table table = new org.apache.hadoop.hive.metastore.api.Table();
    table.setDbName("default");
    table.setTableName("avro_tbl");
    Map<String, String> params = new HashMap<>();
    if (schemaUrl != null) {
      params.put(AvroTableProperties.SCHEMA_URL.getPropName(), schemaUrl);
    }
    if (schemaLiteral != null) {
      params.put(AvroTableProperties.SCHEMA_LITERAL.getPropName(), schemaLiteral);
    }
    table.setParameters(params);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(new ArrayList<FieldSchema>());
    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setSerializationLib(AvroSerDe.class.getName());
    sd.setSerdeInfo(serdeInfo);
    table.setSd(sd);
    return table;
  }
}
