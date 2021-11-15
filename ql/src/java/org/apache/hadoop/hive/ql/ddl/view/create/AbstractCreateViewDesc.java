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

package org.apache.hadoop.hive.ql.ddl.view.create;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * Abstract ancestor of view creating DDL commands.
 */
abstract class AbstractCreateViewDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String viewName;
  private final List<FieldSchema> schema;
  private final String originalText;
  private final String expandedText;

  /**
   * Used to create a view descriptor.
   */
  AbstractCreateViewDesc(String viewName, List<FieldSchema> schema, String originalText, String expandedText) {
    this.viewName = viewName;
    this.schema = schema;
    this.originalText = originalText;
    this.expandedText = expandedText;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getViewName() {
    return viewName;
  }

  @Explain(displayName = "original text", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getOriginalText() {
    return originalText;
  }

  @Explain(displayName = "expanded text")
  public String getExpandedText() {
    return expandedText;
  }

  @Explain(displayName = "columns")
  public List<String> getSchemaString() {
    return Utilities.getFieldSchemaString(schema);
  }

  public List<FieldSchema> getSchema() {
    return schema;
  }
}
