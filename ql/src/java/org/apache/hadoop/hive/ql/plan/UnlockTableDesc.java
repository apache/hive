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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * UnlockTableDesc.
 *
 */
@Explain(displayName = "Unlock Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class UnlockTableDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private String tableName;
  private Map<String, String> partSpec;

  public UnlockTableDesc() {
  }

  public UnlockTableDesc(String tableName, Map<String, String> partSpec) {
    this.tableName = tableName;
    this.partSpec  = partSpec;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Map<String, String> getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(Map<String, String> partSpec) {
    this.partSpec = partSpec;
  }
}
