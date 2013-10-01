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
}
