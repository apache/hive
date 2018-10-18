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

package org.apache.hadoop.hive.accumulo.mr;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.accumulo.AccumuloConnectionParameters;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.serde.AccumuloIndexParameters;
import org.apache.hadoop.hive.accumulo.HiveAccumuloHelper;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;

/**
 *
 */
public class HiveAccumuloTableOutputFormat extends AccumuloIndexedOutputFormat {

  protected final HiveAccumuloHelper helper = new HiveAccumuloHelper();

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    configureAccumuloOutputFormat(job);

    super.checkOutputSpecs(ignored, job);
  }

  @Override
  public RecordWriter<Text, Mutation> getRecordWriter(FileSystem ignored, JobConf job, String name,
                                                     Progressable progress) throws IOException {
    configureAccumuloOutputFormat(job);

    return super.getRecordWriter(ignored, job, name, progress);
  }

  protected void configureAccumuloOutputFormat(JobConf job) throws IOException {
    AccumuloConnectionParameters cnxnParams = getConnectionParams(job);

    final String tableName = job.get(AccumuloSerDeParameters.TABLE_NAME);

    // Make sure we actually go the table name
    Preconditions.checkNotNull(tableName,
        "Expected Accumulo table name to be provided in job configuration");

    // Set the necessary Accumulo information
    try {
      if (cnxnParams.useMockInstance()) {
        getHelper().setOutputFormatMockInstance(job, cnxnParams.getAccumuloInstanceName());
      } else {
        // Accumulo instance name with ZK quorum
        getHelper().setOutputFormatZooKeeperInstance(job, cnxnParams.getAccumuloInstanceName(),
            cnxnParams.getZooKeepers(), cnxnParams.useSasl());
      }

      // Extract the delegation Token from the UGI and add it to the job
      // The AccumuloOutputFormat will look for it there.
      if (cnxnParams.useSasl()) {
        getHelper().updateOutputFormatConfWithAccumuloToken(job, getCurrentUser(), cnxnParams);
      } else {
        getHelper().setOutputFormatConnectorInfo(job, cnxnParams.getAccumuloUserName(),
            new PasswordToken(cnxnParams.getAccumuloPassword()));
      }

      // Set the table where we're writing this data
      setDefaultAccumuloTableName(job, tableName);

      // Set the index table information
      final String indexTableName = job.get(AccumuloIndexParameters.INDEXTABLE_NAME);
      final String indexedColumns = job.get(AccumuloIndexParameters.INDEXED_COLUMNS);
      final String columnTypes = job.get(serdeConstants.LIST_COLUMN_TYPES);
      final boolean binaryEncoding = ColumnEncoding.BINARY.getName()
          .equalsIgnoreCase(job.get(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE));
      setAccumuloIndexTableName(job, indexTableName);
      setAccumuloIndexColumns(job, indexedColumns);
      setAccumuloStringEncoding(job, !binaryEncoding);
    } catch (AccumuloSecurityException e) {
      log.error("Could not connect to Accumulo with provided credentials", e);
      throw new IOException(e);
    }
  }

  // Non-static methods to wrap the static AccumuloOutputFormat methods to enable testing

  protected void setDefaultAccumuloTableName(JobConf conf, String tableName) {
    AccumuloIndexedOutputFormat.setDefaultTableName(conf, tableName);
  }

  protected void setAccumuloIndexTableName(JobConf conf, String indexTableName) {
    AccumuloIndexedOutputFormat.setIndexTableName(conf, indexTableName);
  }

  protected void setAccumuloIndexColumns(JobConf conf, String indexColumns) {
    AccumuloIndexedOutputFormat.setIndexColumns(conf, indexColumns);
  }

  protected void setAccumuloStringEncoding(JobConf conf, Boolean isStringEncoded) {
    AccumuloIndexedOutputFormat.setStringEncoding(conf, isStringEncoded);
  }

  HiveAccumuloHelper getHelper() {
    // Allows mocking in testing.
    return helper;
  }

  AccumuloConnectionParameters getConnectionParams(JobConf conf) {
    // Allows mocking in testing.
    return new AccumuloConnectionParameters(conf);
  }

  UserGroupInformation getCurrentUser() throws IOException {
    // Allows mocking in testing.
    return UserGroupInformation.getCurrentUser();
  }
}
