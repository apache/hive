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
import org.apache.hadoop.hive.ql.io.parquet.vector.probe.ParquetProbeFilter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

public interface VectorizedColumnReader {
  /**
   * read records with specified size and type into the columnVector
   *
   * @param total      number of records to read into the column vector
   * @param column     column vector where the reader will read data into
   * @param columnType the type of column vector
   * @throws IOException
   */
  void readBatch(
    int total,
    ColumnVector column,
    TypeInfo columnType) throws IOException;

  /**
   * Read {@code total} values, but skip the decoding of values at row positions the caller has
   * marked as filtered-out via {@code probeFilter} -- the value is consumed from the underlying
   * page (so state stays coherent), the corresponding vector slot is left null, and any
   * type-conversion / dictionary lookup that {@link #readBatch(int, ColumnVector, TypeInfo)}
   * would have done is skipped.
   *
   * <p>Used by the {@link VectorizedParquetRecordReader} ProbeDecode path: after the join-key
   * column has been decoded and a hash-table probe has produced a selected-row bitmap, the
   * remaining columns are read via this overload so their values are only fully materialized for
   * rows that will survive the hash join.
   *
   * <p>The default implementation ignores the filter and delegates to the 3-arg overload, so
   * readers that don't participate in probe-decode (list, map, struct, dummy) don't need to
   * override anything. Concrete primitive readers should override this to actually consult the
   * filter.
   *
   * @param probeFilter selected-row bitmap for this batch, or {@code null} to decode every row
   */
  default void readBatch(
    int total,
    ColumnVector column,
    TypeInfo columnType,
    ParquetProbeFilter probeFilter) throws IOException {
    readBatch(total, column, columnType);
  }

  default int[] getDefinitionLevels() {
    return null;
  }
}
