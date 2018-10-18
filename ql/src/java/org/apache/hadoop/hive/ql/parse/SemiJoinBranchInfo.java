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

package org.apache.hadoop.hive.ql.parse;


import org.apache.hadoop.hive.ql.exec.TableScanOperator;

public class SemiJoinBranchInfo {
  private TableScanOperator ts;
  private boolean isHint;
  // Default value is true, however, if an optimization deems this edge
  // important, it should set this to false. This does not guarantee that
  // the edge will stay, however, it increases the chances.
  private boolean shouldRemove;

  public SemiJoinBranchInfo(TableScanOperator ts) {
    this.ts = ts;
    isHint = false;
    shouldRemove = true;
  }

  public SemiJoinBranchInfo(TableScanOperator ts, boolean isHint) {
    this.ts = ts;
    this.isHint = isHint;
    shouldRemove = !isHint;  // If hint is true, shouldRemove is redundant anyway
  }

  public TableScanOperator getTsOp() {
    return ts;
  }

  public boolean getIsHint() {
    return isHint;
  }

  public boolean getShouldRemove() {
    return shouldRemove;
  }

  public void setShouldRemove(boolean shouldRemove) {
    // The state only changes from true->false
    // Once set to false, it may not change back to true
    if (this.shouldRemove) {
      this.shouldRemove = shouldRemove;
    }
  }
}
