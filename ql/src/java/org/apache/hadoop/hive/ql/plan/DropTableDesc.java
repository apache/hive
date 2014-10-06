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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DropTableDesc.
 * TODO: this is currently used for both drop table and drop partitions.
 */
@Explain(displayName = "Drop Table")
public class DropTableDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  public static class PartSpec {
    public PartSpec(ExprNodeGenericFuncDesc partSpec, int prefixLength) {
      this.partSpec = partSpec;
      this.prefixLength = prefixLength;
    }
    public ExprNodeGenericFuncDesc getPartSpec() {
      return partSpec;
    }
    public int getPrefixLength() {
      return prefixLength;
    }
    private static final long serialVersionUID = 1L;
    private ExprNodeGenericFuncDesc partSpec;
    // TODO: see if we can get rid of this... used in one place to distinguish archived parts
    private int prefixLength;
  }

  String tableName;
  ArrayList<PartSpec> partSpecs;
  boolean expectView;
  boolean ifExists;
  boolean ignoreProtection;

  public DropTableDesc() {
  }

  /**
   * @param tableName
   */
  public DropTableDesc(String tableName, boolean expectView, boolean ifExists) {
    this.tableName = tableName;
    this.partSpecs = null;
    this.expectView = expectView;
    this.ifExists = ifExists;
    this.ignoreProtection = false;
  }

  public DropTableDesc(String tableName, Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs,
      boolean expectView, boolean ignoreProtection) {
    this.tableName = tableName;
    this.partSpecs = new ArrayList<PartSpec>(partSpecs.size());
    for (Map.Entry<Integer, List<ExprNodeGenericFuncDesc>> partSpec : partSpecs.entrySet()) {
      int prefixLength = partSpec.getKey();
      for (ExprNodeGenericFuncDesc expr : partSpec.getValue()) {
        this.partSpecs.add(new PartSpec(expr, prefixLength));
      }
    }
    this.ignoreProtection = ignoreProtection;
    this.expectView = expectView;
  }

  /**
   * @return the tableName
   */
  @Explain(displayName = "table")
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName
   *          the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the partSpecs
   */
  public ArrayList<PartSpec> getPartSpecs() {
    return partSpecs;
  }

  /**
   * @return whether or not protection will be ignored for the partition
   */
  public boolean getIgnoreProtection() {
    return ignoreProtection;
  }

  /**
   * @param ignoreProtection
   *          set whether or not protection will be ignored for the partition
   */
   public void setIgnoreProtection(boolean ignoreProtection) {
     this.ignoreProtection = ignoreProtection;
   }

  /**
   * @return whether to expect a view being dropped
   */
  public boolean getExpectView() {
    return expectView;
  }

  /**
   * @param expectView
   *          set whether to expect a view being dropped
   */
  public void setExpectView(boolean expectView) {
    this.expectView = expectView;
  }

  /**
   * @return whether IF EXISTS was specified
   */
  public boolean getIfExists() {
    return ifExists;
  }

  /**
   * @param ifExists
   *          set whether IF EXISTS was specified
   */
  public void setIfExists(boolean ifExists) {
    this.ifExists = ifExists;
  }
}
