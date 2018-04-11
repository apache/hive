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
package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.InputEstimator;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

import java.util.Map;

/**
 * This is just a helper class to test the InputEstimator object used in some Utilities methods.
 */
public class InputEstimatorTestClass implements HiveStorageHandler, InputEstimator {
  private static Estimation expectedEstimation = new Estimation(0, 0);

  public InputEstimatorTestClass() {
  }

  public static void setEstimation(Estimation estimation) {
    expectedEstimation = estimation;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return null;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return null;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return null;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {

  }

  @Override
  public void setConf(Configuration conf) {

  }

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public Estimation estimate(JobConf job, TableScanOperator ts, long remaining) throws HiveException {
    return expectedEstimation;
  }
}
