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

package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
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
}
