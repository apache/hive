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

package org.apache.hadoop.hive.ql.anon.builders;

import org.apache.hadoop.hive.metastore.api.IndexType;

public class CreateIndexStatementBuilder {

  private String indexName;
  private String tableName;
  private String columnName;
  private IndexType indexType;
  private int pageSize;
  private int bufferPoolSize;
  private String pointerType;
  private boolean ifNotExists;

  public CreateIndexStatementBuilder() {
  }

  public CreateIndexStatementBuilder(final String indexName, final String tableName, final String columnName) {
    this.indexName = indexName;
    this.tableName = tableName;
    this.columnName = columnName;
  }

  public CreateIndexStatementBuilder withBtreeOptions(final int pageSize, final int bufferPoolSize, final String pointerType) {
    return withPageSize(pageSize)
      .withBufferPoolSize(bufferPoolSize)
      .withPointerType(pointerType)
      .withIndexType(IndexType.BTREE);
  }

  public CreateIndexStatementBuilder withDirectoryOptions(final String pointerType) {
    return withPointerType(pointerType)
      .withIndexType(IndexType.DIRECTORY);
  }

  public CreateIndexStatementBuilder withTabularOptions() {
    return withIndexType(IndexType.TABULAR);
  }

  public CreateIndexStatementBuilder withIndexName(final String indexName) {
    this.indexName = indexName;
    return this;
  }

  public CreateIndexStatementBuilder withTableName(final String tableName) {
    this.tableName = tableName;
    return this;
  }

  public CreateIndexStatementBuilder withColumnName(final String columnName) {
    this.columnName = columnName;
    return this;
  }

  public CreateIndexStatementBuilder withIndexType(final IndexType indexType) {
    this.indexType = indexType;
    return this;
  }

  public CreateIndexStatementBuilder withPageSize(final int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  public CreateIndexStatementBuilder withBufferPoolSize(final int bufferPoolSize) {
    this.bufferPoolSize = bufferPoolSize;
    return this;
  }

  public CreateIndexStatementBuilder withPointerType(final String pointerType) {
    this.pointerType = pointerType;
    return this;
  }

  public CreateIndexStatementBuilder withIfNotExists() {
    this.ifNotExists = true;
    return this;
  }

  public String build() {
    final StringBuilder sb = new StringBuilder();

    sb.append("CREATE IDENTITY INDEX ");
    if (ifNotExists) {
      sb.append("IF NOT EXISTS ");
    }
    sb.append(indexName);
    sb.append(" ON TABLE ");
    sb.append(tableName).append(" (");
    sb.append(columnName).append(")");
    sb.append(" STORED AS ");
    sb.append(indexType.name());

    if (indexType == IndexType.BTREE) {
      sb.append(" WITH (");
      sb.append("PAGE SIZE = ").append(pageSize).append(", ");
      sb.append("BUFFER POOL SIZE = ").append(bufferPoolSize).append(", ");
      sb.append("POINTER TYPE ").append(pointerType);
      sb.append(")");
    } else if (indexType == IndexType.DIRECTORY) {
      sb.append(" WITH (");
      sb.append("POINTER TYPE ").append(pointerType);
      sb.append(")");
    }

    return sb.toString();
  }

}
