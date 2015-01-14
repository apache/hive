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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.io.api.EncodedColumn;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;


/**
 * A row-by-row iterator for ORC files.
 */
public interface RecordReader {
  /**
   * Does the reader have more rows available.
   * @return true if there are more rows
   * @throws java.io.IOException
   */
  boolean hasNext() throws IOException;

  /**
   * Read the next row.
   * @param previous a row object that can be reused by the reader
   * @return the row that was read
   * @throws java.io.IOException
   */
  Object next(Object previous) throws IOException;

  /**
   * Read the next row batch. The size of the batch to read cannot be controlled
   * by the callers. Caller need to look at VectorizedRowBatch.size of the retunred
   * object to know the batch size read.
   * @param previousBatch a row batch object that can be reused by the reader
   * @return the row batch that was read
   * @throws java.io.IOException
   */
  VectorizedRowBatch nextBatch(VectorizedRowBatch previousBatch) throws IOException;

  /**
   * Get the row number of the row that will be returned by the following
   * call to next().
   * @return the row number from 0 to the number of rows in the file
   * @throws java.io.IOException
   */
  long getRowNumber() throws IOException;

  /**
   * Get the progress of the reader through the rows.
   * @return a fraction between 0.0 and 1.0 of rows read
   * @throws java.io.IOException
   */
  float getProgress() throws IOException;

  /**
   * Release the resources associated with the given reader.
   * @throws java.io.IOException
   */
  void close() throws IOException;

  /**
   * Seek to a particular row number.
   */
  void seekToRow(long rowCount) throws IOException;

  /**
   * TODO: this API is subject to change; on one hand, external code should control the threading
   *       aspects, with ORC method returning one EncodedColumn as it will; on the other, it's
   *       more efficient for ORC to read stripe at once, apply RG-level sarg, etc., and thus
   *       return many EncodedColumn-s.
   *  TODO: assumes the reader is for one stripe, otherwise the signature makes no sense.
   *        Also has no columns passed, because that is in ctor.
   * @param colRgs Bitmasks of what RGs are to be read. Has # of elements equal to the number of
   *               included columns; then each bitmask is rgCount bits long; 0 means "need to read"
   * @param rgCount The length of bitmasks in colRgs.
   * @param sarg Sarg to apply additional filtering to RGs.
   * @param consumer Consumer to pass the results too.
   * @param allocator Allocator to allocate memory.
   */
  void readEncodedColumns(long[][] colRgs, int rgCount,
      Consumer<EncodedColumn<OrcBatchKey>> consumer, LowLevelCache cache);
}
