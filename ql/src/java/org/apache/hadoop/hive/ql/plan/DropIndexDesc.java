/**
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
package org.apache.hadoop.hive.ql.plan;

public class DropIndexDesc {
  
  private static final long serialVersionUID = 1L;
  
  private String indexName;
  
  private String tableName;
  
  /**
   * @param indexName
   * @param tableName
   */
  public DropIndexDesc(String indexName, String tableName) {
    super();
    this.indexName = indexName;
    this.tableName = tableName;
  }

  /**
   * @return index name
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * @param indexName index name
   */
  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  /**
   * @return table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName table name
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

}
