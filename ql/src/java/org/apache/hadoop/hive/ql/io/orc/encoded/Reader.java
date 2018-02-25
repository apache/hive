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

package org.apache.hadoop.hive.ql.io.orc.encoded;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.common.Pool.PoolObjectHelper;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.CompressionCodec;
import org.apache.orc.DataReader;
import org.apache.orc.OrcProto;

/**
 * The interface for reading encoded data from ORC files.
 */
public interface Reader extends org.apache.hadoop.hive.ql.io.orc.Reader {

  /**
   * Creates the encoded reader.
   * @param fileKey File ID to read, to use for cache lookups and such.
   * @param dataCache Data cache to use for cache lookups.
   * @param dataReader Data reader to read data not found in cache (from disk, HDFS, and such).
   * @param pf Pool factory to create object pools.
   * @return The reader.
   */
  EncodedReader encodedReader(Object fileKey, DataCache dataCache, DataReader dataReader,
      PoolFactory pf, IoTrace trace, boolean useCodecPool) throws IOException;

  /** The factory that can create (or return) the pools used by encoded reader. */
  public interface PoolFactory {
    <T> Pool<T> createPool(int size, PoolObjectHelper<T> helper);
    Pool<OrcEncodedColumnBatch> createEncodedColumnBatchPool();
    Pool<ColumnStreamData> createColumnStreamDataPool();
  }

  /** Implementation of EncodedColumnBatch for ORC. */
  public static final class OrcEncodedColumnBatch extends EncodedColumnBatch<OrcBatchKey> {
    /** RG index indicating the data applies for all RGs (e.g. a string dictionary). */
    public static final int ALL_RGS = -1;
    /**
     * All the previous streams are data streams, this and the next ones are index streams.
     * We assume the order will stay the same for backward compat.
     */
    public static final int MAX_DATA_STREAMS = OrcProto.Stream.Kind.ROW_INDEX.getNumber();
    public void init(Object fileKey, int stripeIx, int rgIx, int columnCount) {
      if (batchKey == null) {
        batchKey = new OrcBatchKey(fileKey, stripeIx, rgIx);
      } else {
        batchKey.set(fileKey, stripeIx, rgIx);
      }
      resetColumnArrays(columnCount);
    }

    public void initOrcColumn(int colIx) {
      super.initColumn(colIx, MAX_DATA_STREAMS);
    }

    /**
     * Same as columnData, but for the data that already comes as VRBs.
     * The combination of the two contains all the necessary data,
     */
    protected List<ColumnVector>[] columnVectors;

    @Override
    public void reset() {
      super.reset();
      if (columnVectors == null) return;
      Arrays.fill(columnVectors, null);
    }

    @SuppressWarnings("unchecked")
    public void initColumnWithVectors(int colIx, List<ColumnVector> data) {
      if (columnVectors == null) {
        columnVectors = new List[columnData.length];
      }
      columnVectors[colIx] = data;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void resetColumnArrays(int columnCount) {
      super.resetColumnArrays(columnCount);
      if (columnVectors != null && columnCount == columnVectors.length) {
        Arrays.fill(columnVectors, null);
        return;
      } if (columnVectors != null) {
        columnVectors = new List[columnCount];
      } else {
        columnVectors = null;
      }
    }

    public boolean hasVectors(int colIx) {
      return columnVectors != null && columnVectors[colIx] != null;
    }

    public List<ColumnVector> getColumnVectors(int colIx) {
      if (!hasVectors(colIx)) throw new AssertionError("No data for column " + colIx);
      return columnVectors[colIx];
    }
  }
}
