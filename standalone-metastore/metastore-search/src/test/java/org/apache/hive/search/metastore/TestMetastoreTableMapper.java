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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.index.IndexManager;
import org.apache.hive.search.index.Indexer;
import org.apache.hive.search.inference.EmbedModelRegistry;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.mapping.field.Field;
import org.apache.hive.search.mapping.field.IdField;
import org.apache.hive.search.mapping.field.TextField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestMetastoreTableMapper {

  @Test
  public void tableIdIncludesCatalogDbAndTable() {
    assertEquals("hive.default.orders",
        MetastoreTableMapper.tableId("hive", "default", "orders"));
    assertEquals("hive.default.orders",
        MetastoreTableMapper.tableId(TableName.fromString("default.orders", "hive", "default")));
  }

  @Test
  public void fromTableBuildsSearchTextAndStoredFields() throws Exception {
    Configuration conf = new Configuration(false);
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);

    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("sales");
    table.setTableName("orders");
    table.setOwner("alice");
    table.setTableType("MANAGED_TABLE");
    table.setSd(new StorageDescriptor());
    table.getSd().setLocation("hdfs://warehouse/orders");
    table.getSd().setCols(List.of(
        new FieldSchema("id", "bigint", "order id"),
        new FieldSchema("amount", "double", null)));
    Map<String, String> params = new HashMap<>();
    params.put("comment", "daily orders");
    table.setParameters(params);

    TableDocument document = MetastoreTableMapper.fromTable(table, mapping);
    document = withSearchTextEmbedding(document, mapping, new float[] {0.1f, 0.2f, 0.3f});
    assertEquals("hive.sales.orders", document.idField().value());

    List<Document> luceneDocs = document.toDocuments();
    assertEquals(1, luceneDocs.size());
    Document luceneDoc = luceneDocs.get(0);
    assertTrue(luceneDoc.getFields().size() >= 10);
    assertTrue(luceneDoc.get("_id").contains("hive.sales.orders"));
    assertEquals("sales", luceneDoc.get(MetastoreTableMapper.FIELD_DB));
    assertEquals("orders", luceneDoc.get(MetastoreTableMapper.FIELD_TABLE));
    assertTrue(hasIndexedField(luceneDoc, MetastoreTableMapper.FIELD_TABLE + ".filter"));
    String searchText = luceneDoc.get(MetastoreTableMapper.FIELD_SEARCH_TEXT);
    assertTrue(searchText.contains("orders"));
    assertTrue(searchText.contains("daily orders"));
    assertTrue(searchText.contains("id order id"));
    assertFalse(searchText.contains("amount"));
    assertFalse(searchText.contains("hdfs://"));
    assertFalse(searchText.contains("MANAGED_TABLE"));
    assertFalse(searchText.contains("alice"));
    assertEquals("id bigint order id; amount double", luceneDoc.get(MetastoreTableMapper.FIELD_COLUMNS));
    assertEquals("id amount", luceneDoc.get(MetastoreTableMapper.FIELD_COLUMN_NAMES));
    assertEquals("order id", luceneDoc.get(MetastoreTableMapper.FIELD_COLUMN_COMMENTS));
  }

  @Test
  public void columnSearchFieldsSplitNamesAndComments() throws Exception {
    Configuration conf = new Configuration(false);
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);

    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("sales");
    table.setTableName("orders");
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(List.of(
        new FieldSchema("id", "bigint", "order id"),
        new FieldSchema("amount", "double", null),
        new FieldSchema("status", "string", "fulfillment status")));

    TableDocument document = MetastoreTableMapper.fromTable(table, mapping);
    assertEquals("id amount status", fieldValue(document, MetastoreTableMapper.FIELD_COLUMN_NAMES));
    assertEquals("order id; fulfillment status",
        fieldValue(document, MetastoreTableMapper.FIELD_COLUMN_COMMENTS));
  }

  @Test
  public void searchTextIncludesOnlyCommentedColumns() throws Exception {
    Configuration conf = new Configuration(false);
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);

    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("sales");
    table.setTableName("orders");
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(List.of(
        new FieldSchema("id", "bigint", "order id"),
        new FieldSchema("amount", "double", null),
        new FieldSchema("status", "string", "fulfillment status")));

    TableDocument document = MetastoreTableMapper.fromTable(table, mapping);
    String searchText = fieldValue(document, MetastoreTableMapper.FIELD_SEARCH_TEXT);
    String storedColumns = fieldValue(document, MetastoreTableMapper.FIELD_COLUMNS);

    assertTrue(searchText.contains("id order id"));
    assertTrue(searchText.contains("status fulfillment status"));
    assertFalse(searchText.contains("amount"));
    assertTrue(storedColumns.contains("amount double"));
  }

  @Test
  public void searchTextCapsWideTables() throws Exception {
    Configuration conf = new Configuration(false);
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);

    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("wide");
    table.setTableName("events");
    table.setSd(new StorageDescriptor());
    List<FieldSchema> cols = new ArrayList<>();
    int maxCols = MetastoreTableMapper.MAX_SEARCH_COLUMNS;
    for (int i = 0; i < maxCols + 5; i++) {
      cols.add(new FieldSchema("col" + i, "string", "comment " + i));
    }
    table.getSd().setCols(cols);

    TableDocument document = MetastoreTableMapper.fromTable(table, mapping);
    String searchText = fieldValue(document, MetastoreTableMapper.FIELD_SEARCH_TEXT);
    String storedColumns = fieldValue(document, MetastoreTableMapper.FIELD_COLUMNS);

    assertTrue(searchText.contains("col0 comment 0"));
    assertTrue(searchText.contains("col" + (maxCols - 1) + " comment " + (maxCols - 1)));
    assertFalse(searchText.contains("; col" + maxCols + " comment " + maxCols + ";"));
    assertFalse(searchText.endsWith("; col" + maxCols + " comment " + maxCols));
    assertTrue(searchText.contains("(+5 more)"));
    assertTrue(storedColumns.contains("col" + (maxCols + 4) + " string comment " + (maxCols + 4)));
    assertFalse(storedColumns.contains("(+5 more)"));
  }

  @Test
  public void embedDocumentsPreservesLexicalFields() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(org.apache.hive.search.config.IndexStateConfig.MEMORY, true);
    conf.set(org.apache.hive.search.config.InferenceConfig.MODEL_NAME, "stub-model");
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "stub-model", conf);
    IndexManager indexManager = IndexManager.open(mapping, conf);
    EmbedModelRegistry registry = new EmbedModelRegistry(
        java.util.Map.of("stub-model", new org.apache.hive.search.testutil.StubEmbedModel("stub-model")));
    Indexer indexer = new Indexer(indexManager, registry);
    indexer.initialize();

    Table table = sampleTable();
    Map<String, String> params = new HashMap<>();
    params.put("comment", "daily sales orders");
    table.setParameters(params);

    TableDocument doc = MetastoreTableMapper.fromTable(table, mapping);
    TableDocument embedded = indexer.embedDocuments(java.util.List.of(doc)).get(0);
    java.util.Set<String> fieldNames = new java.util.HashSet<>();
    for (Field field : embedded.fields()) {
      if (field instanceof TextField textField) {
        fieldNames.add(textField.name());
      }
    }
    assertTrue(fieldNames.contains(MetastoreTableMapper.FIELD_COMMENT));
    assertTrue(fieldNames.contains(MetastoreTableMapper.FIELD_SEARCH_TEXT));

    ByteBuffersDirectory directory = new ByteBuffersDirectory();
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(mapping.analyzer()))) {
      writer.addDocuments(embedded.toDocuments());
    }
    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      assertNotNull(MultiTerms.getTerms(reader, MetastoreTableMapper.FIELD_COMMENT));
    }
    indexer.close();
    indexManager.close();
    registry.close();
  }

  @Test
  public void lexicalFieldsAreIndexedForKeywordSearch() throws Exception {
    Configuration conf = new Configuration(false);
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);

    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("sales");
    table.setTableName("orders");
    table.setSd(new StorageDescriptor());
    Map<String, String> params = new HashMap<>();
    params.put("comment", "daily sales orders");
    table.setParameters(params);

    TableDocument document = MetastoreTableMapper.fromTable(table, mapping);
    document = withSearchTextEmbedding(document, mapping, new float[] {0.1f, 0.2f, 0.3f});
    ByteBuffersDirectory directory = new ByteBuffersDirectory();
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(mapping.analyzer()))) {
      writer.addDocuments(document.toDocuments());
    }
    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      assertNotNull(MultiTerms.getTerms(reader, MetastoreTableMapper.FIELD_COMMENT));
      assertNotNull(MultiTerms.getTerms(reader, MetastoreTableMapper.FIELD_TABLE));
      assertNotNull(MultiTerms.getTerms(reader, MetastoreTableMapper.FIELD_TABLE + ".filter"));
    }
  }

  @Test
  public void semanticFieldRequiresEmbedding() throws Exception {
    Configuration conf = new Configuration(false);
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);
    TableDocument document = MetastoreTableMapper.fromTable(sampleTable(), mapping);
    document.appendField(new TextField(MetastoreTableMapper.FIELD_SEARCH_TEXT, "sales data"));
    try {
      document.toDocuments();
      org.junit.Assert.fail("expected semantic field without embedding to fail");
    } catch (org.apache.hive.search.exception.IndexException expected) {
      assertTrue(expected.getMessage().contains("requires embedding"));
    }
  }

  private static Table sampleTable() {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("default");
    table.setTableName("t");
    table.setSd(new StorageDescriptor());
    return table;
  }

  private static TableDocument withSearchTextEmbedding(
      TableDocument document, IndexMapping mapping, float[] embedding) {
    java.util.List<Field> fields = new java.util.ArrayList<>();
    for (Field field : document.fields()) {
      if (field instanceof IdField) {
        continue;
      }
      if (field instanceof TextField textField
          && MetastoreTableMapper.FIELD_SEARCH_TEXT.equals(textField.name())) {
        fields.add(textField.withEmbedding(embedding));
      } else {
        fields.add(field);
      }
    }
    return new TableDocument(document.idField(), fields, mapping);
  }

  private static boolean hasIndexedField(Document document, String fieldName) {
    for (IndexableField field : document.getFields()) {
      if (fieldName.equals(field.name())) {
        return true;
      }
    }
    return false;
  }

  private static String fieldValue(TableDocument document, String fieldName) {
    for (Field field : document.fields()) {
      if (field instanceof TextField textField && fieldName.equals(textField.name())) {
        return textField.value();
      }
    }
    throw new AssertionError("missing field: " + fieldName);
  }
}
