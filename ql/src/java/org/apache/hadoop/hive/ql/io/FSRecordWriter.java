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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.io.Writable;

/**
 * Record writer used by file sink operator.
 *
 * FSRecordWriter.
 *
 */
public interface FSRecordWriter {
  void write(Writable w) throws IOException;

  void close(boolean abort) throws IOException;

  /**
   * If a file format internally gathers statistics (like ORC) while writing then
   * it can expose the statistics through this record writer interface. Writer side
   * statistics is useful for updating the metastore with table/partition level
   * statistics.
   * StatsProvidingRecordWriter.
   *
   */
  public interface StatsProvidingRecordWriter extends FSRecordWriter{
    /**
     * Returns the statistics information
     * @return SerDeStats
     */
    SerDeStats getStats();
  }

}