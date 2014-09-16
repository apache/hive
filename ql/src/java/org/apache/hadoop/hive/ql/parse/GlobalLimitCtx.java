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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.ql.plan.LimitDesc;

/**
 * context for pruning inputs. populated by GlobalLimitOptimizer
 */
public class GlobalLimitCtx {

  private boolean enable;
  private int globalLimit;
  private boolean hasTransformOrUDTF;
  private LimitDesc lastReduceLimitDesc;

  public GlobalLimitCtx() {
    reset();
  }

  public int getGlobalLimit() {
    return globalLimit;
  }

  public boolean ifHasTransformOrUDTF() {
    return hasTransformOrUDTF;
  }

  public void setHasTransformOrUDTF(boolean hasTransformOrUDTF) {
    this.hasTransformOrUDTF = hasTransformOrUDTF;
  }

  public LimitDesc getLastReduceLimitDesc() {
    return lastReduceLimitDesc;
  }

  public void setLastReduceLimitDesc(LimitDesc lastReduceLimitDesc) {
    this.lastReduceLimitDesc = lastReduceLimitDesc;
  }

  public boolean isEnable() {
    return enable;
  }

  public void enableOpt(int globalLimit) {
    this.enable = true;
    this.globalLimit = globalLimit;
  }

  public void disableOpt() {
    this.enable = false;
    this.globalLimit = -1;
    this.lastReduceLimitDesc = null;
  }

  public void reset() {
    enable = false;
    globalLimit = -1;
    hasTransformOrUDTF = false;
    lastReduceLimitDesc = null;
  }
}
