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
import org.apache.hive.search.config.InferenceOptions;
import org.apache.hive.search.config.SearchOptions;
import org.apache.hive.search.mapping.BinaryFieldSchema;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TextFieldSchema;

public final class MetastoreIndexSchema {

  private MetastoreIndexSchema() {}

  public static IndexMapping tableIndexMapping(Configuration conf) {
    Map<String, FieldSchema> fields = new LinkedHashMap<>();
    fields.put(MetastoreTableMapper.FIELD_DB, filterLexicalText(MetastoreTableMapper.FIELD_DB));
    fields.put(MetastoreTableMapper.FIELD_TABLE, filterLexicalText(MetastoreTableMapper.FIELD_TABLE));

    fields.put(MetastoreTableMapper.FIELD_OWNER, filterText(MetastoreTableMapper.FIELD_OWNER));
    fields.put(MetastoreTableMapper.FIELD_TABLE_TYPE, filterText(MetastoreTableMapper.FIELD_TABLE_TYPE));

    fields.put(MetastoreTableMapper.FIELD_LOCATION, lexicalText(MetastoreTableMapper.FIELD_LOCATION));
    fields.put(MetastoreTableMapper.FIELD_COMMENT, lexicalText(MetastoreTableMapper.FIELD_COMMENT));
    fields.put(MetastoreTableMapper.FIELD_COLUMNS, lexicalText(MetastoreTableMapper.FIELD_COLUMNS));
    fields.put(MetastoreTableMapper.FIELD_COLUMN_COMMENTS,
        lexicalText(MetastoreTableMapper.FIELD_COLUMN_COMMENTS));

    int segmentMax = new SearchOptions(conf).getSemanticSegmentMax();
    String model = new InferenceOptions(conf).embedderName();
    for (int i = 0; i < segmentMax; i++) {
      String name = SearchTextSegment.segmentField(i);
      fields.put(name, semanticText(name, model));
    }
    fields.put(MetastoreTableMapper.FIELD_TABLE_BLOB,
        new BinaryFieldSchema(MetastoreTableMapper.FIELD_TABLE_BLOB));
    return new IndexMapping(conf, fields);
  }

  private static TextFieldSchema filterText(String name) {
    return new TextFieldSchema(name, false).storeField(false).filterField(true);
  }

  private static TextFieldSchema lexicalText(String name) {
    return new TextFieldSchema(name, true).storeField(true).filterField(false);
  }

  private static TextFieldSchema filterLexicalText(String name) {
    return new TextFieldSchema(name, true).filterField(true).storeField(false);
  }

  private static TextFieldSchema semanticText(String name, String model) {
    return new TextFieldSchema(name, false).semanticField(model).filterField(false).storeField(false);
  }
}
