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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.config.SearchOptions;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.SearchParams;

public final class MetastoreIndexSchema {

  private MetastoreIndexSchema() {}

  public static IndexMapping defaultHiveTablesMapping(String indexName,
      String semanticModel, Configuration conf) {
    Map<String, FieldSchema> fields = new LinkedHashMap<>();
    fields.put(MetastoreTableMapper.FIELD_DB, filterText(MetastoreTableMapper.FIELD_DB));
    fields.put(MetastoreTableMapper.FIELD_TABLE, tableNameText(MetastoreTableMapper.FIELD_TABLE));
    fields.put(MetastoreTableMapper.FIELD_OWNER, filterText(MetastoreTableMapper.FIELD_OWNER));
    fields.put(MetastoreTableMapper.FIELD_TABLE_TYPE, filterText(MetastoreTableMapper.FIELD_TABLE_TYPE));
    fields.put(MetastoreTableMapper.FIELD_LOCATION, storedText(MetastoreTableMapper.FIELD_LOCATION));
    fields.put(MetastoreTableMapper.FIELD_COMMENT, lexicalText(MetastoreTableMapper.FIELD_COMMENT));
    fields.put(MetastoreTableMapper.FIELD_COLUMNS, lexicalText(MetastoreTableMapper.FIELD_COLUMNS));
    fields.put(
        MetastoreTableMapper.FIELD_COLUMN_COMMENTS,
        lexicalText(MetastoreTableMapper.FIELD_COLUMN_COMMENTS));
    int segmentMax = new SearchOptions(conf).getSemanticSegmentMax();
    for (int i = 0; i < segmentMax; i++) {
      String name = SearchTextSegment.segmentField(i);
      if (i == 0) {
        fields.put(name, hybridText(name, semanticModel));
      } else {
        fields.put(name, semanticText(name, semanticModel));
      }
    }
    return new IndexMapping(indexName, conf, fields);
  }

  private static FieldSchema.TextFieldSchema filterText(String name) {
    return new FieldSchema.TextFieldSchema(name, SearchParams.disabled(), true, true);
  }

  private static FieldSchema.TextFieldSchema storedText(String name) {
    return new FieldSchema.TextFieldSchema(name, SearchParams.disabled(), true, false);
  }

  private static FieldSchema.TextFieldSchema lexicalText(String name) {
    return new FieldSchema.TextFieldSchema(
        name, new SearchParams(true, null, SearchParams.VectorDistance.COSINE), true, false);
  }

  private static FieldSchema.TextFieldSchema tableNameText(String name) {
    return new FieldSchema.TextFieldSchema(
        name, new SearchParams(true, null, SearchParams.VectorDistance.COSINE), true, true);
  }

  private static FieldSchema.TextFieldSchema hybridText(String name, String semanticModel) {
    return new FieldSchema.TextFieldSchema(
        name,
        new SearchParams(true, semanticModel, SearchParams.VectorDistance.COSINE),
        false,
        false);
  }

  private static FieldSchema.TextFieldSchema semanticText(String name, String semanticModel) {
    return new FieldSchema.TextFieldSchema(
        name,
        new SearchParams(false, semanticModel, SearchParams.VectorDistance.COSINE),
        false,
        false);
  }
}
