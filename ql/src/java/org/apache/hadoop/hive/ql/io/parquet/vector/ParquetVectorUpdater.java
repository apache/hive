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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

import java.io.IOException;

/**
 * Converts the values of one Parquet column into a Hive column vector, for one pairing of Parquet and Hive type.
 * The caller decodes the definition levels first: values go only to the rows whose {@code isNull} is false.
 */
interface ParquetVectorUpdater {

  /** Sets up the vector for a batch. */
  void beginBatch(ColumnVector column);

  /** Whether PLAIN pages read in bulk from {@link VectorizedPlainValuesReader}; other pages read per value. */
  default boolean readsPlainInBulk() {
    return true;
  }

  /** Reads the {@code nonNull} values of rows {@code [offset, offset + total)} from the current page. */
  void readValues(int total, int nonNull, int offset, ColumnVector column, VectorizedPlainValuesReader values)
      throws IOException;

  /** Fills the non-NULL rows of {@code [offset, offset + total)} from dictionary ids {@code ids[0 .. nonNull)}. */
  void decodeDictionaryIds(int total, int nonNull, int offset, ColumnVector column, int[] ids);

  /**
   * Whether a batch of {@code total} rows, none of them NULL, holds a single value. {@code dictionaryOnly} says every
   * row came from a dictionary page, and then {@code sameIds} whether every row read the same id.
   */
  boolean isRepeating(ColumnVector column, int total, boolean dictionaryOnly, boolean sameIds);
}
