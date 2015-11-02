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
public interface RecordReader extends org.apache.orc.RecordReader {
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
}
