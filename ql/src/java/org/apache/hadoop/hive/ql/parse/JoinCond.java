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

/**
 * Join conditions Descriptor implementation.
 * 
 */
public class JoinCond {
  private int left;
  private int right;
  private JoinType joinType;
  private boolean preserved;

  public JoinCond() {
  }

  public JoinCond(int left, int right, JoinType joinType) {
    this.left = left;
    this.right = right;
    this.joinType = joinType;
  }

  /**
   * Constructor for a UNIQUEJOIN cond.
   * 
   * @param p
   *          true if table is preserved, false otherwise
   */
  public JoinCond(boolean p) {
    joinType = org.apache.hadoop.hive.ql.parse.JoinType.UNIQUE;
    preserved = p;
  }

  /**
   * @return the true if table is preserved, false otherwise
   */
  public boolean getPreserved() {
    return preserved;
  }

  public int getLeft() {
    return left;
  }

  public void setLeft(final int left) {
    this.left = left;
  }

  public int getRight() {
    return right;
  }

  public void setRight(final int right) {
    this.right = right;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public void setJoinType(final JoinType joinType) {
    this.joinType = joinType;
  }

  public void setPreserved(boolean preserved) {
    this.preserved = preserved;
  }
}
