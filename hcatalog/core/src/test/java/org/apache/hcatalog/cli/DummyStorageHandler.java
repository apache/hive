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
package org.apache.hcatalog.cli;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.mapreduce.HCatStorageHandler;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.cli.DummyStorageHandler} instead
 */
class DummyStorageHandler extends HCatStorageHandler {

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public void setConf(Configuration conf) {
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return DummyInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return DummyOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return ColumnarSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider()
    throws HiveException {
    return new DummyAuthProvider();
  }
  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    //do nothing by default
    //EK: added the same (no-op) implementation as in
    // org.apache.hive.hcatalog.DefaultStorageHandler (hive 0.12)
    // this is needed to get 0.11 API compat layer to work
    // see HIVE-4896
  }

  private class DummyAuthProvider implements HiveAuthorizationProvider {

    @Override
    public Configuration getConf() {
      return null;
    }

    /* @param conf
     * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public void setConf(Configuration conf) {
    }

    /* @param conf
    /* @throws HiveException
     * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#init(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public void init(Configuration conf) throws HiveException {
    }

    /* @return HiveAuthenticationProvider
     * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#getAuthenticator()
     */
    @Override
    public HiveAuthenticationProvider getAuthenticator() {
      return null;
    }

    /* @param authenticator
     * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#setAuthenticator(org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider)
     */
    @Override
    public void setAuthenticator(HiveAuthenticationProvider authenticator) {
    }

    /* @param readRequiredPriv
    /* @param writeRequiredPriv
    /* @throws HiveException
    /* @throws AuthorizationException
     * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#authorize(org.apache.hadoop.hive.ql.security.authorization.Privilege[], org.apache.hadoop.hive.ql.security.authorization.Privilege[])
     */
    @Override
    public void authorize(Privilege[] readRequiredPriv,
                Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException {
    }

    /* @param db
    /* @param readRequiredPriv
    /* @param writeRequiredPriv
    /* @throws HiveException
    /* @throws AuthorizationException
     * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#authorize(org.apache.hadoop.hive.metastore.api.Database, org.apache.hadoop.hive.ql.security.authorization.Privilege[], org.apache.hadoop.hive.ql.security.authorization.Privilege[])
     */
    @Override
    public void authorize(Database db, Privilege[] readRequiredPriv,
                Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException {
    }

    /* @param table
    /* @param readRequiredPriv
    /* @param writeRequiredPriv
    /* @throws HiveException
    /* @throws AuthorizationException
     * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#authorize(org.apache.hadoop.hive.ql.metadata.Table, org.apache.hadoop.hive.ql.security.authorization.Privilege[], org.apache.hadoop.hive.ql.security.authorization.Privilege[])
     */
    @Override
    public void authorize(org.apache.hadoop.hive.ql.metadata.Table table, Privilege[] readRequiredPriv,
                Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException {
    }

    /* @param part
    /* @param readRequiredPriv
    /* @param writeRequiredPriv
    /* @throws HiveException
    /* @throws AuthorizationException
     * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#authorize(org.apache.hadoop.hive.ql.metadata.Partition, org.apache.hadoop.hive.ql.security.authorization.Privilege[], org.apache.hadoop.hive.ql.security.authorization.Privilege[])
     */
    @Override
    public void authorize(Partition part, Privilege[] readRequiredPriv,
                Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException {
    }

    /* @param table
    /* @param part
    /* @param columns
    /* @param readRequiredPriv
    /* @param writeRequiredPriv
    /* @throws HiveException
    /* @throws AuthorizationException
     * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#authorize(org.apache.hadoop.hive.ql.metadata.Table, org.apache.hadoop.hive.ql.metadata.Partition, java.util.List, org.apache.hadoop.hive.ql.security.authorization.Privilege[], org.apache.hadoop.hive.ql.security.authorization.Privilege[])
     */
    @Override
    public void authorize(org.apache.hadoop.hive.ql.metadata.Table table, Partition part, List<String> columns,
                Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    }

  }

  /**
   * The Class DummyInputFormat is a dummy implementation of the old hadoop
   * mapred.InputFormat required by HiveStorageHandler.
   */
  class DummyInputFormat implements
    InputFormat<WritableComparable, HCatRecord> {

    /*
     * @see
     * org.apache.hadoop.mapred.InputFormat#getRecordReader(org.apache.hadoop
     * .mapred.InputSplit, org.apache.hadoop.mapred.JobConf,
     * org.apache.hadoop.mapred.Reporter)
     */
    @Override
    public RecordReader<WritableComparable, HCatRecord> getRecordReader(
      InputSplit split, JobConf jobconf, Reporter reporter)
      throws IOException {
      throw new IOException("This operation is not supported.");
    }

    /*
     * @see
     * org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.
     * mapred .JobConf, int)
     */
    @Override
    public InputSplit[] getSplits(JobConf jobconf, int number)
      throws IOException {
      throw new IOException("This operation is not supported.");
    }
  }

  /**
   * The Class DummyOutputFormat is a dummy implementation of the old hadoop
   * mapred.OutputFormat and HiveOutputFormat required by HiveStorageHandler.
   */
  class DummyOutputFormat implements
    OutputFormat<WritableComparable<?>, HCatRecord>,
    HiveOutputFormat<WritableComparable<?>, HCatRecord> {

    /*
     * @see
     * org.apache.hadoop.mapred.OutputFormat#checkOutputSpecs(org.apache
     * .hadoop .fs.FileSystem, org.apache.hadoop.mapred.JobConf)
     */
    @Override
    public void checkOutputSpecs(FileSystem fs, JobConf jobconf)
      throws IOException {
      throw new IOException("This operation is not supported.");

    }

    /*
     * @see
     * org.apache.hadoop.mapred.OutputFormat#getRecordWriter(org.apache.
     * hadoop .fs.FileSystem, org.apache.hadoop.mapred.JobConf,
     * java.lang.String, org.apache.hadoop.util.Progressable)
     */
    @Override
    public RecordWriter<WritableComparable<?>, HCatRecord> getRecordWriter(
      FileSystem fs, JobConf jobconf, String str,
      Progressable progress) throws IOException {
      throw new IOException("This operation is not supported.");
    }

    /*
     * @see
     * org.apache.hadoop.hive.ql.io.HiveOutputFormat#getHiveRecordWriter(org
     * .apache.hadoop.mapred.JobConf, org.apache.hadoop.fs.Path,
     * java.lang.Class, boolean, java.util.Properties,
     * org.apache.hadoop.util.Progressable)
     */
    @Override
    public FSRecordWriter getHiveRecordWriter(
      JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress)
      throws IOException {
      throw new IOException("This operation is not supported.");
    }

  }

}


