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

package org.apache.hadoop.hive.ql.io.parquet.vector.probe;

import java.io.IOException;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

/**
 * Row-batch probe against a small-table hash table for the Parquet ProbeDecode path.
 *
 * <p>Given a {@link ColumnVector} that carries the just-decoded join-key column, implementations
 * probe each row against the small-side hash table and produce a {@link ParquetProbeFilter} whose
 * bitmap marks the rows that survive the join. Subsequent non-key columns in the same batch are
 * then read via {@code VectorizedColumnReader.readBatch(..., ParquetProbeFilter)} so their values
 * skip decode / conversion work for filtered-out rows.
 *
 * <p>Structured after the ORC-side {@code OrcProbeHashTable} family: the key type-specialisations
 * (single long/int, single string, multi-key) live in their own subclasses so the per-row probe
 * stays a monomorphic virtual call. This first cut ships only the long/int variant
 * ({@link ParquetProbeLongHashTable}); the string and multi-key variants will land later.
 */
public interface ParquetProbeHashTable {

  /**
   * Probe every row in {@code keyColumn} (positions 0..batchSize-1) against the small-side hash
   * table and return a filter over the surviving rows. Rows marked null in the key column never
   * match (join semantics: NULL != NULL).
   *
   * @param keyColumn key-column vector, already decoded by the primary readBatch() call
   * @param batchSize number of rows in the current batch
   * @return a bitmap-form {@link ParquetProbeFilter} of length {@code batchSize}
   */
  ParquetProbeFilter probe(ColumnVector keyColumn, int batchSize) throws IOException;
}
