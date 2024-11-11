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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.metasummary;

import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metasummary.MetaSummaryHandler;
import org.apache.hadoop.hive.metastore.metasummary.MetaSummarySchema;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg specific implementation of summary collector
 */
public class IcebergSummaryHandler implements MetaSummaryHandler {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSummaryHandler.class);
  private HiveCatalog catalog;
  private Configuration configuration;
  // Bunch of actual summary retrievers for the Iceberg table.
  private List<IcebergSummaryRetriever> summaryRetrievers;
  private UserGroupInformation ugi;

  @Override
  public void initialize(String catalogName, boolean jsonFormat, MetaSummarySchema schema)
      throws SummaryInitializationException {
    Objects.requireNonNull(schema, "schema is null");
    String mgdWarehouse = MetastoreConf.getAsString(configuration, MetastoreConf.ConfVars.WAREHOUSE);
    String extWarehouse = MetastoreConf.getAsString(configuration, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL);
    String uris = MetastoreConf.getAsString(configuration, MetastoreConf.ConfVars.THRIFT_URIS);
    Map<String, String> propertiesMap = Maps.newHashMap();
    LOG.info("Initializing iceberg summary handler with warehouse:{}，external warehouse:{}，uris:{}",
        mgdWarehouse, extWarehouse, uris);
    propertiesMap.put("warehouse", mgdWarehouse);
    propertiesMap.put("externalwarehouse", extWarehouse);
    propertiesMap.put("uri", uris);
    PrivilegedAction<HiveCatalog> action = () -> {
      HiveCatalog hiveCatalog = new HiveCatalog();
      hiveCatalog.setConf(configuration);
      hiveCatalog.initialize(catalogName, propertiesMap);
      return hiveCatalog;
    };
    String hadoopAuth = configuration.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "simple");
    if ("kerberos".equalsIgnoreCase(hadoopAuth)) {
      this.ugi = loginKerberosUser();
      this.catalog = this.ugi.doAs(action);
    } else {
      this.catalog = action.run();
    }
    this.summaryRetrievers = Lists.newArrayList();
    this.summaryRetrievers.addAll(Arrays.asList(
        new PuffinStatisticsSummary(),
        new MetadataSummary(),
        new TablePropertySummary()
    ));
    this.summaryRetrievers.forEach(retriever -> {
      retriever.initialize(configuration, jsonFormat);
      schema.addFields(retriever.getFieldNames());
    });
  }

  private UserGroupInformation loginKerberosUser() throws SummaryInitializationException {
    // Use the principal from metastore
    String principal = MetastoreConf.getAsString(configuration, MetastoreConf.ConfVars.KERBEROS_PRINCIPAL);
    String keytabFile = MetastoreConf.getAsString(configuration, MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE);
    if (StringUtils.isEmpty(principal)) {
      throw new SummaryInitializationException("No principal specified from property: " +
          MetastoreConf.ConfVars.KERBEROS_PRINCIPAL.getHiveName());
    }
    if (StringUtils.isEmpty(keytabFile)) {
      throw new SummaryInitializationException("No keytab specified from property: " +
          MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE.getHiveName());
    }
    try {
      UserGroupInformation.setConfiguration(configuration);
      String kerberosName = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
      UserGroupInformation.loginUserFromKeytab(kerberosName, keytabFile);
      return UserGroupInformation.getLoginUser();
    } catch (Exception e) {
      LOG.error("Failed to log the user: {} with keytab: {}", principal, keytabFile);
      throw new SummaryInitializationException("Error while logging the user: " + principal, e);
    }
  }

  @Override
  public void appendSummary(TableName tableName, MetadataTableSummary tableSummary) {
    Objects.requireNonNull(tableName);
    if (summaryRetrievers == null || catalog == null) {
      throw new IllegalStateException("The Iceberg summary handler hasn't been initialized yet!");
    }
    LOG.debug("Starting to collect the summary for the table: {}", tableName);
    final TableIdentifier tblId = TableIdentifier.of(tableName.getDb(), tableName.getTable());
    PrivilegedAction<Long> action = () -> {
      long start = System.currentTimeMillis();
      Table table;
      try {
        table = catalog.loadTable(tblId);
        this.summaryRetrievers.forEach(retriever ->
            retriever.getMetaSummary(table, tableSummary));
      } catch (Exception e) {
        LOG.warn("Error while loading the table: " + tableName, e);
        tableSummary.setDropped(true);
      }
      return System.currentTimeMillis() - start;
    };
    long timeSpent = ugi != null ? ugi.doAs(action) : action.run();
    LOG.debug("Finished the summary collection in {} ms for table: {}", timeSpent, tableName);
  }

  /**
   * Close the summary handler
   */
  @Override
  public void close() throws Exception {
    if (catalog != null) {
      catalog.close();
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.configuration = new Configuration(Objects.requireNonNull(conf, "conf is null"));
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }
}
