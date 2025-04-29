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

import java.util.ArrayList;
import java.util.List;

/**
 *
 * MColumnDescriptor.
 * A wrapper around a list of columns.
 */
public class MColumnDescriptor {
  private List<MColumn> columns;
  private long id;

  public MColumnDescriptor() {}

  public MColumnDescriptor(List<MFieldSchema> cols) {
    columns = new ArrayList<>(cols.size());
    for (MFieldSchema schema : cols) {
      columns.add(new MColumn(schema.getName(), schema.getType(), schema.getComment()));
    }
  }

  public List<MColumn> getColumns() {
    return columns;
  }

  public void setColumns(List<MColumn> columns) {
    this.columns = columns;
  }

  public List<MFieldSchema> getCols() {
    List<MFieldSchema> schemas = new ArrayList<>();
    if (getColumns() != null) {
      for (MColumn column : getColumns()) {
        schemas.add(new MFieldSchema(column.getName(), column.getType(), column.getComment()));
      }
    }
    return schemas;
  }

  public long getId() {
    return id;
  }
}
