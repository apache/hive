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

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * An abstraction of keys within a VectorizedRowBatch.
 *
 * A key is one or more columns.
 *
 * When there is a sequential "run" of equal keys, they are collapsed and represented by a
 * duplicate count.
 *
 * The batch of keys (with sequential duplicates collapsed) is called a series.
 *
 * A key can be all null, or a key with no or some nulls.
 *
 * All keys have a duplicate count.
 *
 * A key with no or some nulls has:
 *   1) A hash code.
 *   2) Key values and other value(s) defined by other interfaces.
 *
 * The key series is logically indexed.  That is, if batch.selectedInUse is true, the indices
 * will be logical and need to be mapped through batch.selected to get the physical batch
 * indices.  Otherwise, the indices are physical batch indices.
 */
public interface VectorKeySeries {

  /**
   * Process a non-empty batch of rows and compute a key series.
   *
   * The key series will be positioned to the beginning.
   *
   * @param batch
   * @throws IOException
   */
  void processBatch(VectorizedRowBatch batch) throws IOException;

  /**
   * Position to the beginning of the key series.
   */
  void positionToFirst();

  /**
   * @return the current logical index of the first row of the current key.
   * The next duplicate count rows have the same key.
   */
  int getCurrentLogical();

  /**
   * @return true when the current key is all nulls.
   */
  boolean getCurrentIsAllNull();

  /**
   * @return the number of duplicate keys of the current key.
   */
  int getCurrentDuplicateCount();


  /**
   * @return true when there is at least one null in the current key.
   * Only valid when getCurrentIsAllNull is false.  Otherwise, undefined.
   */
  boolean getCurrentHasAnyNulls();

  /**
   * @return the hash code of the current key.
   * Only valid when getCurrentIsAllNull is false.  Otherwise, undefined.
   */
  int getCurrentHashCode();

  /**
   * Move to the next key.
   * @return true when there is another key.  Otherwise, the key series is complete.
   */
  boolean next();
}