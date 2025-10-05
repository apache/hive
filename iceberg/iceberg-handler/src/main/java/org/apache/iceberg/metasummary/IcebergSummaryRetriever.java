/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.metasummary;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.iceberg.Table;

/**
 * Interface for the Iceberg table to collect the real summary
 */
public abstract class IcebergSummaryRetriever {
  protected boolean formatJson;
  protected Configuration configuration;

  /**
   * Initialize the retriever
   * @param conf The input configuration
   * @param json Whether the summary is printed as json or not
   */
  public void initialize(Configuration conf, boolean json) {
    this.configuration = conf;
    this.formatJson = json;
  }

  /**
   * Fetch extra fields from this particular retriever
   * @return Extra fields need to be added
   */
  public abstract List<String> getFieldNames();

  /**
   * Retrieve the summary from the table
   * @param table The Iceberg table
   * @param summary The table's summary to be updated
   */
  public abstract void getMetaSummary(Table table, MetadataTableSummary summary);

}
