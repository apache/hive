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

import java.io.Serializable;

import org.apache.hadoop.hive.ql.plan.Explain;



/**
 *
 * This class stores all the information specified in the TABLESAMPLE(PERCENT ...) clause.
 * e.g. for the clause "FROM t TABLESAMPLE(1 PERCENT) it will store the percentage 1,
 * and the seed number is to determine which 1%. Currently it is from the conf
 * hive.sample.seednumber
 *
 */
public class SplitSample implements Serializable{

  private static final long serialVersionUID = 1L;

  /**
   * The percentage of the TABLESAMPLE clause.
   */
  private double percent;

  /**
   * The number used to determine which part of the input to sample
   */
  private int seedNum = 0;

  public SplitSample() {
  }


  public SplitSample(double percent, int seedNum) {
    this.percent = percent;
    this.seedNum = seedNum;
  }

  @Explain(displayName = "percentage")
  public double getPercent() {
    return percent;
  }

  public void setPercent(double percent) {
    this.percent = percent;
  }

  @Explain(displayName = "seed number")
  public int getSeedNum() {
    return seedNum;
  }

  public void setSeedNum(int seedNum) {
    this.seedNum = seedNum;
  }

}
