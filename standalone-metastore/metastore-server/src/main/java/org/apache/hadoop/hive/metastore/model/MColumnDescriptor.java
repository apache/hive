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

/**
 *
 */
package org.apache.hadoop.hive.metastore.model;

import javax.jdo.annotations.NotPersistent;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.List;

/**
 *
 * MColumnDescriptor.
 * A wrapper around a list of columns.
 */
public class MColumnDescriptor {
  private List<MColumn> fields;

  @NotPersistent
  private List<MFieldSchema> columns;
  private long id;

  public MColumnDescriptor() {}

  public MColumnDescriptor(List<MFieldSchema> cols) {
    fields = cols.stream().map(schema ->
        new MColumn(schema.getName(), schema.getType(), schema.getComment()))
            .collect(Collectors.toList());
  }

  public List<MColumn> getFields() {
    return fields;
  }

  public List<MFieldSchema> getCols() {
    if (columns != null) {
      return columns;
    }
    columns = new ArrayList<>();
    if (fields != null) {
      columns = fields.stream().map(column ->
              new MFieldSchema(column.getName(), column.getType(), column.getComment()))
          .collect(Collectors.toList());
    }
    return columns;
  }

  public long getId() {
    return id;
  }
}
