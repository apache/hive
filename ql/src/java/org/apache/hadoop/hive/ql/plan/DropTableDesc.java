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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DropTableDesc.
 *
 */
@Explain(displayName = "Drop Table")
public class DropTableDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  String tableName;
  ArrayList<LinkedHashMap<String, String>> partSpecs;
  boolean expectView;

  public DropTableDesc() {
  }

  /**
   * @param tableName
   */
  public DropTableDesc(String tableName, boolean expectView) {
    this.tableName = tableName;
    partSpecs = null;
    this.expectView = expectView;
  }

  public DropTableDesc(String tableName, List<? extends Map<String, String>> partSpecs) {
    this.tableName = tableName;
    this.partSpecs = new ArrayList<LinkedHashMap<String, String>>(partSpecs.size());
    for (int i = 0; i < partSpecs.size(); i++) {
      this.partSpecs.add(new LinkedHashMap<String, String>(partSpecs.get(i)));
    }
    expectView = false;
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
  public ArrayList<LinkedHashMap<String, String>> getPartSpecs() {
    return partSpecs;
  }

  /**
   * @param partSpecs
   *          the partSpecs to set
   */
  public void setPartSpecs(ArrayList<LinkedHashMap<String, String>> partSpecs) {
    this.partSpecs = partSpecs;
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
}
