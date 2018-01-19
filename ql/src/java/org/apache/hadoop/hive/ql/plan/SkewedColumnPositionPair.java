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
/**
 * This class record 2 types of positions for a skewed column:
 * 1. position in table column list
 * 2. position in skewed column list
 * Position starts from 0.
 * For example, create a table with
 * create table list_bucketing_static_part (key String, value String)
 * partitioned by (ds String, hr String)
 * skewed by (value) on ('val_466','val_287','val_82')
 * stored as DIRECTORIES
 * STORED AS RCFILE;
 *
 * Skewed column is "value".
 * 1. It's position in table column is 1.
 * 2. It's position in skewed column list is 0.
 *
 * This information will be used in {@FileSinkOperator} generateListBucketingDirName
 */
public class SkewedColumnPositionPair {
  private int tblColPosition;
  private int skewColPosition;

  public SkewedColumnPositionPair () {}

  public SkewedColumnPositionPair (int tblColPosition, int skewColPosition) {
    this.tblColPosition = tblColPosition;
    this.skewColPosition = skewColPosition;
  }

  /**
   * @return the tblColPosition
   */
  public int getTblColPosition() {
    return tblColPosition;
  }

  /**
   * @param tblColPosition the tblColPosition to set
   */
  public void setTblColPosition(int tblColPosition) {
    this.tblColPosition = tblColPosition;
  }

  /**
   * @return the skewColPosition
   */
  public int getSkewColPosition() {
    return skewColPosition;
  }

  /**
   * @param skewColPosition the skewColPosition to set
   */
  public void setSkewColPosition(int skewColPosition) {
    this.skewColPosition = skewColPosition;
  }

}
