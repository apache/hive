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

package org.apache.hadoop.hive.ql.metadata;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

/**
 * Parameters for {@link HiveStorageHandler#createOrReplaceNativeView} (native catalog view DDL).
 */
public final class CreateNativeViewRequest implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String databaseName;
  private final String viewName;
  private final List<FieldSchema> schema;
  private final String expandedText;
  private final Map<String, String> properties;
  private final String comment;
  private final boolean replace;
  private final boolean ifNotExists;

  public CreateNativeViewRequest(
      String databaseName,
      String viewName,
      List<FieldSchema> schema,
      String expandedText,
      Map<String, String> properties,
      String comment,
      boolean replace,
      boolean ifNotExists) {
    this.databaseName = databaseName;
    this.viewName = viewName;
    this.schema = schema;
    this.expandedText = expandedText;
    this.properties = properties == null ? null : Collections.unmodifiableMap(properties);
    this.comment = comment;
    this.replace = replace;
    this.ifNotExists = ifNotExists;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getViewName() {
    return viewName;
  }

  public List<FieldSchema> getSchema() {
    return schema;
  }

  public String getExpandedText() {
    return expandedText;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public String getComment() {
    return comment;
  }

  public boolean isReplace() {
    return replace;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }
}
