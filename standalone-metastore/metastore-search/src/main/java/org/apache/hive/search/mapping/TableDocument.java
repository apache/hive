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

import java.util.ArrayList;
import java.util.List;

import org.apache.hive.search.exception.IndexIOException;
import org.apache.hive.search.mapping.field.Field;
import org.apache.hive.search.mapping.field.IdField;
import org.apache.hive.search.mapping.field.TextField;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;

public class TableDocument {
  private static final int MAX_FIELD_SEARCH_SIZE = 32_768;

  private final List<Field> fields;
  private final Document document;
  private final IndexMapping indexMapping;
  private final IdField idField;
  public static final String FILTER_SUFFIX = ".filter";

  public TableDocument(IdField idf, List<Field> otherFields, IndexMapping mapping) {
    idField = idf;
    indexMapping = mapping;
    fields = new ArrayList<>(1 + otherFields.size());
    fields.add(idField);
    fields.addAll(otherFields);
    document = new Document();
  }

  public void fill(TextField field, FieldSchema.TextFieldSchema schema)
      throws IndexIOException {
    String trimmed = trim(field.value(), MAX_FIELD_SEARCH_SIZE);

    if (schema.filter()) {
      document.add(
          new StringField(field.name() + FILTER_SUFFIX, field.value(),
              org.apache.lucene.document.Field.Store.NO));
    }
    if (schema.search().lexical()) {
      org.apache.lucene.document.Field.Store store =
          schema.store() ? org.apache.lucene.document.Field.Store.YES
              : org.apache.lucene.document.Field.Store.NO;
      document.add(new org.apache.lucene.document.TextField(field.name(), trimmed, store));
    } else if (schema.store()) {
      document.add(new StoredField(field.name(), field.value()));
    }
    if (schema.search().semantic()) {
      if (field.embedding() == null) {
        throw new IndexIOException("semantic field '" + field.name() + "' requires embedding");
      }
      VectorSimilarityFunction similarity = schema.search().distance() == SearchParams.VectorDistance.DOT ?
          VectorSimilarityFunction.DOT_PRODUCT : VectorSimilarityFunction.COSINE;
      document.add(new KnnFloatVectorField(field.name(), field.embedding(), similarity));
    }
  }

  private static String trim(String value, int max) {
    return value.length() > max ? value.substring(0, max) : value;
  }

  public List<Document> toDocuments() throws IndexIOException {
    String id = idField().value();
    document.add(new BinaryDocValuesField("_id" + FILTER_SUFFIX, new BytesRef(id)));
    document.add(new StoredField("_id", id));
    document.add(new StringField("_id" + FILTER_SUFFIX, id,
        org.apache.lucene.document.Field.Store.NO));
    for (Field field : fields) {
      if (field instanceof IdField) {
        continue;
      }
      FieldSchema schema = indexMapping.fieldSchema(field.name());
      if (schema == null) {
        continue;
      }
      if (schema instanceof FieldSchema.TextFieldSchema text
          && field instanceof TextField tf) {
        fill(tf, text);
      }
    }
    return List.of(document);
  }

  public void appendField(Field field) {
    fields.add(field);
  }

  public IdField idField() {
    return idField;
  }

  public List<Field> fields() {
    return fields;
  }
}
