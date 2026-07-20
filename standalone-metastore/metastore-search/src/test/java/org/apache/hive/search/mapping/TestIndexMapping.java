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

package org.apache.hive.search.mapping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.metastore.MetastoreSchemas;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestIndexMapping {

  @Test
  public void defaultSchemaExposesSingleHybridField() {
    Configuration conf = new Configuration(false);
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);

    assertEquals(1, mapping.hybridFields().size());
    assertTrue(mapping.soleHybridField().isPresent());
    assertEquals(MetastoreTableMapper.FIELD_SEARCH_TEXT, mapping.soleHybridField().get());
  }

  @Test
  public void lexicalFieldsAreConfiguredInDefaultSchema() {
    Configuration conf = new Configuration(false);
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);
    FieldSchema tableSchema = mapping.fieldSchema(MetastoreTableMapper.FIELD_TABLE);
    FieldSchema commentSchema = mapping.fieldSchema(MetastoreTableMapper.FIELD_COMMENT);
    assertTrue(tableSchema instanceof FieldSchema.TextFieldSchema table
        && table.search().lexical());
    assertTrue(commentSchema instanceof FieldSchema.TextFieldSchema comment
        && comment.search().lexical());
    FieldSchema columnCommentsSchema = mapping.fieldSchema(MetastoreTableMapper.FIELD_COLUMN_COMMENTS);
    assertTrue(columnCommentsSchema instanceof FieldSchema.TextFieldSchema columnComments
        && columnComments.search().lexical());
    assertNotNull(mapping.analyzer());
  }
}
