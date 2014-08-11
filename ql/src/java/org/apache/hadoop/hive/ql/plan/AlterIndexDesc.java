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

import java.io.Serializable;
import java.util.Map;

/**
 * AlterIndexDesc.
 *
 */
@Explain(displayName = "Alter Index")
public class AlterIndexDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private String indexName;
  private String baseTable;
  private Map<String, String> partSpec; // partition specification of partitions touched
  private Map<String, String> props;

  /**
   * alterIndexTypes.
   *
   */
  public static enum AlterIndexTypes {
    UPDATETIMESTAMP,
    ADDPROPS};

  AlterIndexTypes op;

  public AlterIndexDesc() {
  }

  public AlterIndexDesc(AlterIndexTypes type) {
    this.op = type;
  }

  /**
   * @return the name of the index
   */
  @Explain(displayName = "name")
  public String getIndexName() {
    return indexName;
  }

  /**
   * @param indexName
   *          the indexName to set
   */
  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  /**
   * @return the baseTable
   */
  @Explain(displayName = "new name")
  public String getBaseTableName() {
    return baseTable;
  }

  /**
   * @param baseTable
   *          the baseTable to set
   */
  public void setBaseTableName(String baseTable) {
    this.baseTable = baseTable;
  }

  /**
   * @return the partition spec
   */
  public Map<String, String> getSpec() {
    return partSpec;
  }

  /**
   * @param partSpec
   *          the partition spec to set
   */
  public void setSpec(Map<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  /**
   * @return the op
   */
  public AlterIndexTypes getOp() {
    return op;
  }

  /**
   * @param op
   *          the op to set
   */
  public void setOp(AlterIndexTypes op) {
    this.op = op;
  }

  /**
   * @return the props
   */
  @Explain(displayName = "properties")
  public Map<String, String> getProps() {
    return props;
  }

  /**
   * @param props
   *          the props to set
   */
  public void setProps(Map<String, String> props) {
    this.props = props;
  }
}
