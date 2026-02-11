/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.metasummary;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.common.TableName;

/**
 * Interface for collecting the meta summary for the non-native tables.
 */
public interface MetaSummaryHandler extends AutoCloseable, Configurable {

  /**
   * Initialize the handler, it should be invoked only once before calling the appendSummary
   * @param catalog The catalog name
   * @param formatJson True if the summary is printed as json, false otherwise.
   * @param schema Extra summary fields
   */
  void initialize(String catalog,
      boolean formatJson, MetaSummarySchema schema) throws SummaryInitializationException;

  /**
   * Perform the summary collection
   * @param tableName The input table
   * @param tableSummary The original summary collected from HMS for this table, which needs to be updated.
   */
  void appendSummary(TableName tableName, MetadataTableSummary tableSummary);

  /**
   * The exception throws from the summary initialization
   */
  class SummaryInitializationException extends RuntimeException {
    public SummaryInitializationException(String message) {
      super(message);
    }

    public SummaryInitializationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
