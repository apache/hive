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

package org.apache.hadoop.hive.ql.log.syslog;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * SyslogStorageHandler handles log files written to logs table in sys db. sys.logs is external table partitioned by
 * dt (date), ns (namespace) and app (application) where logs files are stored in YYYY-mm-DD-HH-MM_i[.log][.gz] format.
 * The contents in the log files are expected to be in RFC5424 layout for machine readability and
 * human readability (some editors can replace the unescape the linebreaks correctly to show stacktraces etc.).
 * NOTE: This storage handler is not generic and has several assumptions that are specific for hive's own log
 * processing. Refer {@link SyslogInputFormat} and {@link SyslogParser} for more assumptions.
 */
public class SyslogStorageHandler implements HiveStorageHandler {
  private Configuration conf;

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return SyslogInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return TextOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return SyslogSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    // no hook by default
    return null;
  }

  public HiveAuthorizationProvider getAuthorizationProvider() {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc,
    Map<String, String> jobProperties) {
    // do nothing by default
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
    Map<String, String> jobProperties) {
    // do nothing by default
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
    Map<String, String> jobProperties) {
    //do nothing by default
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    //do nothing by default
  }

  @Override
  public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> secrets) {
    //do nothing by default
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
