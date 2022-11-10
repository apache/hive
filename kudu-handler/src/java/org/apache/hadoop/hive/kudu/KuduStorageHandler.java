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
package org.apache.hadoop.hive.kudu;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.kudu.KuduOutputFormat.KuduRecordWriter;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.metadata.StorageHandlerInfo;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a HiveStorageHandler implementation for Apache Kudu.
 */
public class KuduStorageHandler extends DefaultStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(KuduStorageHandler.class);

  private static final String KUDU_PREFIX = "kudu:";

  private static final String KUDU_PROPERTY_PREFIX = "kudu.";

  /** Table Properties. Used in the hive table definition when creating a new table. */
  public static final String KUDU_TABLE_ID_KEY = KUDU_PROPERTY_PREFIX + "table_id";
  public static final String KUDU_TABLE_NAME_KEY = KUDU_PROPERTY_PREFIX + "table_name";
  public static final String KUDU_MASTER_ADDRS_KEY = KUDU_PROPERTY_PREFIX + "master_addresses";
  public static final List<String> KUDU_TABLE_PROPERTIES =
      Arrays.asList(KUDU_TABLE_ID_KEY, KUDU_TABLE_NAME_KEY, KUDU_MASTER_ADDRS_KEY);

  private Configuration conf;

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return KuduInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return KuduOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return KuduSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return new DefaultHiveAuthorizationProvider();
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
  public void configureInputJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    // Copied from the DruidStorageHandler.
    if (UserGroupInformation.isSecurityEnabled()) {
      // AM can not do Kerberos Auth so will do the input split generation in the HS2
      LOG.debug("Setting {} to {} to enable split generation on HS2",
          HiveConf.ConfVars.HIVE_AM_SPLIT_GENERATION.toString(),
          Boolean.FALSE.toString());
      jobConf.set(HiveConf.ConfVars.HIVE_AM_SPLIT_GENERATION.toString(), Boolean.FALSE.toString());
    }
    try {
      Utilities.addDependencyJars(jobConf, KuduStorageHandler.class);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  private void configureJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {

    Properties tblProps = tableDesc.getProperties();
    copyPropertiesFromTable(jobProperties, tblProps);
  }

  private void copyPropertiesFromTable(Map<String, String> jobProperties, Properties tblProps) {
    for (String propToCopy : KUDU_TABLE_PROPERTIES) {
      if (tblProps.containsKey(propToCopy)) {
        String value = tblProps.getProperty(propToCopy);
        conf.set(propToCopy, value);
        jobProperties.put(propToCopy, value);
      }
    }
  }

  @Override
  public URI getURIForAuth(Table table) throws URISyntaxException {
    Map<String, String> tableProperties = HiveCustomStorageHandlerUtils.getTableProperties(table);
    String host_name = tableProperties.get(KUDU_MASTER_ADDRS_KEY) != null ?
            tableProperties.get(KUDU_MASTER_ADDRS_KEY) : conf.get(KUDU_MASTER_ADDRS_KEY);
    Preconditions.checkNotNull(host_name, "Set Table property " + conf);
    String table_name = tableProperties.get(KUDU_TABLE_NAME_KEY);
    Preconditions.checkNotNull(table_name, "Set Table property " + KUDU_TABLE_NAME_KEY);
    return new URI(KUDU_PREFIX+"//"+host_name+"/"+table_name);
  }

  /**
   * Gives the storage handler a chance to decompose a predicate.
   * The storage handler should analyze the predicate and return the portion of it which
   * cannot be evaluated during table access.
   *
   * @param jobConf contains a job configuration matching the one that will later be passed
   *               to getRecordReader and getSplits
   * @param deserializer deserializer which will be used when fetching rows
   * @param predicate predicate to be decomposed
   * @return decomposed form of predicate, or null if no pushdown is possible at all
   */
  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
                                                ExprNodeDesc predicate) {
    Preconditions.checkArgument(deserializer instanceof KuduSerDe);
    KuduSerDe serDe = (KuduSerDe) deserializer;
    Schema schema = serDe.getSchema();
    return KuduPredicateHandler.decompose(predicate, schema);
  }

  /**
   * Used to fetch runtime information about storage handler during DESCRIBE EXTENDED statement.
   */
  @Override
  public StorageHandlerInfo getStorageHandlerInfo(Table table) throws MetaException {
    return null;
  }
}
