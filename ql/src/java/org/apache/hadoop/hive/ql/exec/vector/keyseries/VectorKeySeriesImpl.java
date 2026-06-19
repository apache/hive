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

package org.apache.hadoop.hive.ql.exec.vector.keyseries;

/**
 * A base implementation of VectorKeySeries.
 *
 */
public abstract class VectorKeySeriesImpl implements VectorKeySeries {

  protected int currentLogical;
  protected boolean currentIsAllNull;
  protected boolean currentHasAnyNulls;
  protected int currentDuplicateCount;
  protected int currentHashCode;

  VectorKeySeriesImpl() {
    currentLogical = 0;
    currentIsAllNull = false;

    // Set to true by default.  Only actively set in the multiple key case to support Outer Join.
    currentHasAnyNulls = true;

    currentDuplicateCount = 0;
    currentHashCode = 0;
  }

  @Override
  public int getCurrentLogical() {
    return currentLogical;
  }

  @Override
  public boolean getCurrentIsAllNull() {
    return currentIsAllNull;
  }

  @Override
  public boolean getCurrentHasAnyNulls() {
    return currentHasAnyNulls;
  }

  @Override
  public int getCurrentDuplicateCount() {
    return currentDuplicateCount;
  }

  @Override
  public int getCurrentHashCode() {
    return currentHashCode;
  }
}