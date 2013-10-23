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

package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * The InputFormat to use to read data from HCatalog.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HCatInputFormat extends HCatBaseInputFormat {

  private Configuration conf;
  private InputJobInfo inputJobInfo;

  /**
   * Initializes the input with a null filter.
   * See {@link #setInput(org.apache.hadoop.conf.Configuration, String, String, String)}
   */
  public static HCatInputFormat setInput(
          Job job, String dbName, String tableName)
    throws IOException {
    return setInput(job.getConfiguration(), dbName, tableName, null);
  }

  /**
   * Initializes the input with a provided filter.
   * See {@link #setInput(org.apache.hadoop.conf.Configuration, String, String, String)}
   */
  public static HCatInputFormat setInput(
          Job job, String dbName, String tableName, String filter)
    throws IOException {
    return setInput(job.getConfiguration(), dbName, tableName, filter);
  }

  /**
   * Initializes the input with a null filter.
   * See {@link #setInput(org.apache.hadoop.conf.Configuration, String, String, String)}
   */
  public static HCatInputFormat setInput(
          Configuration conf, String dbName, String tableName)
    throws IOException {
    return setInput(conf, dbName, tableName, null);
  }

  /**
   * Set inputs to use for the job. This queries the metastore with the given input
   * specification and serializes matching partitions into the job conf for use by MR tasks.
   * @param conf the job configuration
   * @param dbName database name, which if null 'default' is used
   * @param tableName table name
   * @param filter the partition filter to use, can be null for no filter
   * @throws IOException on all errors
   */
  public static HCatInputFormat setInput(
          Configuration conf, String dbName, String tableName, String filter)
    throws IOException {

    Preconditions.checkNotNull(conf, "required argument 'conf' is null");
    Preconditions.checkNotNull(tableName, "required argument 'tableName' is null");

    HCatInputFormat hCatInputFormat = new HCatInputFormat();
    hCatInputFormat.conf = conf;
    hCatInputFormat.inputJobInfo = InputJobInfo.create(dbName, tableName, filter, null);

    try {
      InitializeInput.setInput(conf, hCatInputFormat.inputJobInfo);
    } catch (Exception e) {
      throw new IOException(e);
    }

    return hCatInputFormat;
  }

  /**
   * @deprecated As of 0.13
   * Use {@link #setInput(org.apache.hadoop.conf.Configuration, String, String, String)} instead,
   * to specify a partition filter to directly initialize the input with.
   */
  @Deprecated
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
