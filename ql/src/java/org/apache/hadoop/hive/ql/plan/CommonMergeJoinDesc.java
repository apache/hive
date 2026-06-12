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
import java.util.List;

import org.apache.hadoop.hive.ql.plan.Explain.Level;


@Explain(displayName = "Merge Join Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CommonMergeJoinDesc extends MapJoinDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private int numBuckets;
  private int mapJoinConversionPos;

  /**
   * Human-readable key column names per join position (indexed by alias/tag).
   * Populated at compile time so they are available at task runtime for skew monitoring.
   */
  private String[] skewJoinKeyNames;

  /**
   * Human-readable table names per join position (indexed by alias/tag).
   * Populated at compile time so they are available at task runtime for skew monitoring.
   */
  private String[] skewJoinTableAliases;

  CommonMergeJoinDesc() {
  }

  public CommonMergeJoinDesc(int numBuckets, int mapJoinConversionPos,
      MapJoinDesc joinDesc) {
    super(joinDesc);

    for (List<ExprNodeDesc> keys : joinDesc.getKeys().values()) {
      if (!isSupportedComplexType(keys)) {
        //TODO : https://issues.apache.org/jira/browse/HIVE-25042
        throw new RuntimeException("map and union type is not supported for common merge join");
      }
    }

    this.numBuckets = numBuckets;
    this.mapJoinConversionPos = mapJoinConversionPos;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public int getBigTablePosition() {
    return mapJoinConversionPos;
  }

  public void setBigTablePosition(int pos) {
    mapJoinConversionPos = pos;
  }

  public String[] getSkewJoinKeyNames() {
    return skewJoinKeyNames;
  }

  public void setSkewJoinKeyNames(String[] skewJoinKeyNames) {
    this.skewJoinKeyNames = skewJoinKeyNames;
  }

  public String[] getSkewJoinTableAliases() {
    return skewJoinTableAliases;
  }

  public void setSkewJoinTableAliases(String[] skewJoinTableAliases) {
    this.skewJoinTableAliases = skewJoinTableAliases;
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (super.isSame(other)) {
      CommonMergeJoinDesc otherDesc = (CommonMergeJoinDesc) other;
      return getNumBuckets() == otherDesc.getNumBuckets();
    }
    return false;
  }
}
