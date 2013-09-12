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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * The InputFormat to use to read data from HCatalog.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.HCatInputFormat} instead
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HCatInputFormat extends HCatBaseInputFormat {

  private Configuration conf;
  private InputJobInfo inputJobInfo;

  /**
   * @deprecated as of release 0.5, and will be removed in a future release
   */
  @Deprecated
  public static void setInput(Job job, InputJobInfo inputJobInfo) throws IOException {
    setInput(job.getConfiguration(), inputJobInfo);
  }

  /**
   * @deprecated as of release 0.5, and will be removed in a future release
   */
  @Deprecated
  public static void setInput(Configuration conf, InputJobInfo inputJobInfo) throws IOException {
    setInput(conf, inputJobInfo.getDatabaseName(), inputJobInfo.getTableName())
      .setFilter(inputJobInfo.getFilter())
      .setProperties(inputJobInfo.getProperties());
  }

  /**
   * See {@link #setInput(org.apache.hadoop.conf.Configuration, String, String)}
   */
  public static HCatInputFormat setInput(Job job, String dbName, String tableName) throws IOException {
    return setInput(job.getConfiguration(), dbName, tableName);
  }

  /**
   * Set inputs to use for the job. This queries the metastore with the given input
   * specification and serializes matching partitions into the job conf for use by MR tasks.
   * @param conf the job configuration
   * @param dbName database name, which if null 'default' is used
   * @param tableName table name
   * @throws IOException on all errors
   */
  public static HCatInputFormat setInput(Configuration conf, String dbName, String tableName)
    throws IOException {

    Preconditions.checkNotNull(conf, "required argument 'conf' is null");
    Preconditions.checkNotNull(tableName, "required argument 'tableName' is null");

    HCatInputFormat hCatInputFormat = new HCatInputFormat();
    hCatInputFormat.conf = conf;
    hCatInputFormat.inputJobInfo = InputJobInfo.create(dbName, tableName, null, null);

    try {
      InitializeInput.setInput(conf, hCatInputFormat.inputJobInfo);
    } catch (Exception e) {
      throw new IOException(e);
    }

    return hCatInputFormat;
  }

  /**
   * Set a filter on the input table.
   * @param filter the filter specification, which may be null
   * @return this
   * @throws IOException on all errors
   */
  public HCatInputFormat setFilter(String filter) throws IOException {
    // null filters are supported to simplify client code
    if (filter != null) {
      inputJobInfo = InputJobInfo.create(
        inputJobInfo.getDatabaseName(),
        inputJobInfo.getTableName(),
        filter,
        inputJobInfo.getProperties());
      try {
        InitializeInput.setInput(conf, inputJobInfo);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    return this;
  }

  /**
   * Set properties for the input format.
   * @param properties properties for the input specification
   * @return this
   * @throws IOException on all errors
   */
  public HCatInputFormat setProperties(Properties properties) throws IOException {
    Preconditions.checkNotNull(properties, "required argument 'properties' is null");
    inputJobInfo = InputJobInfo.create(
      inputJobInfo.getDatabaseName(),
      inputJobInfo.getTableName(),
      inputJobInfo.getFilter(),
      properties);
    try {
      InitializeInput.setInput(conf, inputJobInfo);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return this;
  }
}
