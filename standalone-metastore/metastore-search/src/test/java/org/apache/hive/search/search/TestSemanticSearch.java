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

package org.apache.hive.search.search;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.SearchParams;
import org.apache.hive.search.metastore.MetastoreSchemas;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@Category(MetastoreUnitTest.class)
public class TestSemanticSearch {

  private static IndexMapping defaultMapping() {
    Configuration conf = new Configuration(false);
    return MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);
  }

  @Test
  public void resolveStringUsesSoleSemanticField() throws Exception {
    SemanticSearch.ResolvedSemanticQuery resolved =
        SemanticSearch.resolve(new SearchArgs.Semantic("sales revenue", null), defaultMapping());
    assertEquals(MetastoreTableMapper.FIELD_SEARCH_TEXT, resolved.field());
    assertEquals("sales revenue", resolved.queryText());
  }

  @Test
  public void resolveExplicitField() throws Exception {
    SemanticSearch.ResolvedSemanticQuery resolved = SemanticSearch.resolve(
        new SearchArgs.Semantic("orders", MetastoreTableMapper.FIELD_SEARCH_TEXT),
        defaultMapping());
    assertEquals(MetastoreTableMapper.FIELD_SEARCH_TEXT, resolved.field());
    assertEquals("orders", resolved.queryText());
  }

  @Test
  public void resolveRejectsNonSemanticField() {
    assertThrows(
        SearchException.class,
        () ->
            SemanticSearch.resolve(
                new SearchArgs.Semantic("t1", MetastoreTableMapper.FIELD_TABLE),
                defaultMapping()));
  }

  @Test
  public void resolveRequiresFieldWhenMultipleSemanticFieldsExist() {
    Configuration conf = new Configuration(false);
    Map<String, FieldSchema> fields = new LinkedHashMap<>();
    fields.put(
        "field_a",
        new FieldSchema.TextFieldSchema(
            "field_a", new SearchParams(true, "model-a", SearchParams.VectorDistance.COSINE)));
    fields.put(
        "field_b",
        new FieldSchema.TextFieldSchema(
            "field_b", new SearchParams(true, "model-b", SearchParams.VectorDistance.COSINE)));
    IndexMapping mapping = new IndexMapping("idx", conf, fields);

    assertThrows(
        SearchException.class,
        () -> SemanticSearch.resolve(new SearchArgs.Semantic("query", null), mapping));
  }
}
