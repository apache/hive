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

package org.apache.hadoop.hive.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.mr.HiveAccumuloTableInputFormat;
import org.apache.hadoop.hive.accumulo.mr.HiveAccumuloTableOutputFormat;
import org.apache.hadoop.hive.accumulo.predicate.AccumuloPredicateHandler;
import org.apache.hadoop.hive.accumulo.serde.AccumuloIndexParameters;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDe;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Create table mapping to Accumulo for Hive. Handle predicate pushdown if necessary.
 */
public class AccumuloStorageHandler extends DefaultStorageHandler implements HiveMetaHook,
    HiveStoragePredicateHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AccumuloStorageHandler.class);
  private static final String DEFAULT_PREFIX = "default";

  protected AccumuloPredicateHandler predicateHandler = AccumuloPredicateHandler.getInstance();
  protected AccumuloConnectionParameters connectionParams;
  protected Configuration conf;
  protected HiveAccumuloHelper helper = new HiveAccumuloHelper();

  /**
   * Push down table properties into the JobConf.
   *
   * @param desc
   *          Hive table description
   * @param jobProps
   *          Properties that will be added to the JobConf by Hive
   */
  @Override
  public void configureTableJobProperties(TableDesc desc, Map<String, String> jobProps) {
    // Should not be getting invoked, configureInputJobProperties or configureOutputJobProperties
    // should be invoked instead.
    configureInputJobProperties(desc, jobProps);
    configureOutputJobProperties(desc, jobProps);
  }

  protected String getTableName(Table table) throws MetaException {
    // Use TBLPROPERTIES
    String tableName = table.getParameters().get(AccumuloSerDeParameters.TABLE_NAME);

    if (null != tableName) {
      return tableName;
    }

    // Then try SERDEPROPERTIES
    tableName = table.getSd().getSerdeInfo().getParameters()
        .get(AccumuloSerDeParameters.TABLE_NAME);

    if (null != tableName) {
      return tableName;
    }

    // Use the hive table name, ignoring the default database
    if (DEFAULT_PREFIX.equals(table.getDbName())) {
      return table.getTableName();
    } else {
      return table.getDbName() + "." + table.getTableName();
    }
  }

  protected String getIndexTableName(Table table) {
    // Use TBLPROPERTIES
    String idxTableName = table.getParameters().get(AccumuloIndexParameters.INDEXTABLE_NAME);

    if (null != idxTableName) {
      return idxTableName;
    }

    // Then try SERDEPROPERTIES
    idxTableName = table.getSd().getSerdeInfo().getParameters()
        .get(AccumuloIndexParameters.INDEXTABLE_NAME);

    return idxTableName;
  }

  protected String getTableName(TableDesc tableDesc) {
    Properties props = tableDesc.getProperties();
    String tableName = props.getProperty(AccumuloSerDeParameters.TABLE_NAME);
    if (null != tableName) {
      return tableName;
    }

    tableName = props.getProperty(hive_metastoreConstants.META_TABLE_NAME);

    if (tableName.startsWith(DEFAULT_PREFIX + ".")) {
      return tableName.substring(DEFAULT_PREFIX.length() + 1);
    }

    return tableName;
  }

  protected String getColumnTypes(TableDesc tableDesc)  {
    Properties props = tableDesc.getProperties();
    String columnTypes = props.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    return columnTypes;
  }

  protected String getIndexTableName(TableDesc tableDesc) {
    Properties props = tableDesc.getProperties();
    String tableName = props.getProperty(AccumuloIndexParameters.INDEXTABLE_NAME);
    return tableName;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    connectionParams = new AccumuloConnectionParameters(conf);
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return AccumuloSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties props = tableDesc.getProperties();

    jobProperties.put(AccumuloSerDeParameters.COLUMN_MAPPINGS,
        props.getProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS));

    String tableName = props.getProperty(AccumuloSerDeParameters.TABLE_NAME);
    if (null == tableName) {
      tableName = getTableName(tableDesc);
    }
    jobProperties.put(AccumuloSerDeParameters.TABLE_NAME,
        tableName);

    String useIterators = props.getProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY);
    if (useIterators != null) {
      if (!"true".equalsIgnoreCase(useIterators) && !"false".equalsIgnoreCase(useIterators)) {
        throw new IllegalArgumentException("Expected value of true or false for "
            + AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY);
      }

      jobProperties.put(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, useIterators);
    }

    String storageType = props.getProperty(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE);
    if (null != storageType) {
      jobProperties.put(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE, storageType);
    }

    String authValue = props.getProperty(AccumuloSerDeParameters.AUTHORIZATIONS_KEY);
    if (null != authValue) {
      jobProperties.put(AccumuloSerDeParameters.AUTHORIZATIONS_KEY, authValue);
    }

    LOG.info("Computed input job properties of " + jobProperties);

    Configuration conf = getConf();
    helper.loadDependentJars(conf);

    // When Kerberos is enabled, we have to add the Accumulo delegation token to the
    // Job so that it gets passed down to the YARN/Tez task.
    if (connectionParams.useSasl()) {
      try {
        // Open an accumulo connection
        Connector conn = connectionParams.getConnector();

        // Convert the Accumulo token in a Hadoop token
        Token<? extends TokenIdentifier> accumuloToken = helper.setConnectorInfoForInputAndOutput(connectionParams, conn, conf);

        // Probably don't have a JobConf here, but we can still try...
        if (conf instanceof JobConf) {
          // Convert the Accumulo token in a Hadoop token
          LOG.debug("Adding Hadoop Token for Accumulo to Job's Credentials: " + accumuloToken);

          // Add the Hadoop token to the JobConf
          JobConf jobConf = (JobConf) conf;
          jobConf.getCredentials().addToken(accumuloToken.getService(), accumuloToken);
          LOG.info("All job tokens: " + jobConf.getCredentials().getAllTokens());
        } else {
          LOG.info("Don't have a JobConf, so we cannot persist Tokens. Have to do it later.");
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to obtain DelegationToken for "
            + connectionParams.getAccumuloUserName(), e);
      }
    }
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties props = tableDesc.getProperties();
    // Adding these job properties will make them available to the OutputFormat in checkOutputSpecs
    String colMap = props.getProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS);
    jobProperties.put(AccumuloSerDeParameters.COLUMN_MAPPINGS, colMap);

    String tableName = props.getProperty(AccumuloSerDeParameters.TABLE_NAME);
    if (null == tableName) {
      tableName = getTableName(tableDesc);
    }
    jobProperties.put(AccumuloSerDeParameters.TABLE_NAME, tableName);

    String indexTable = props.getProperty(AccumuloIndexParameters.INDEXTABLE_NAME);
    if (null == indexTable) {
      indexTable = getIndexTableName(tableDesc);
    }

    if ( null != indexTable) {
      jobProperties.put(AccumuloIndexParameters.INDEXTABLE_NAME, indexTable);

      String indexColumns = props.getProperty(AccumuloIndexParameters.INDEXED_COLUMNS);
      jobProperties.put(AccumuloIndexParameters.INDEXED_COLUMNS,
          getIndexedColFamQuals(tableDesc, indexColumns, colMap));
    }

    if (props.containsKey(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE)) {
      jobProperties.put(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE,
          props.getProperty(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE));
    }

    if (props.containsKey(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY)) {
      jobProperties.put(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY,
          props.getProperty(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY));
    }
  }

  private String getIndexedColFamQuals(TableDesc tableDesc, String indexColumns, String colMap) {
    StringBuilder sb = new StringBuilder();

    String cols = indexColumns;


    String hiveColString = tableDesc.getProperties().getProperty(serdeConstants.LIST_COLUMNS);
    // if there are actual accumulo index columns defined then build
    // the comma separated list of accumulo columns
    if (cols == null || cols.isEmpty() || "*".equals(indexColumns)) {
      // skip rowid
      cols = hiveColString.substring(hiveColString.indexOf(',')+1);
    }

    String[] hiveTypes = tableDesc.getProperties()
        .getProperty(serdeConstants.LIST_COLUMN_TYPES).split(":");
    String[] accCols = colMap.split(",");
    String[] hiveCols = hiveColString.split(",");
    Set<String> indexSet = new HashSet<String>();

    for (String idx : cols.split(",")) {
      indexSet.add(idx.trim());
    }

    for (int i = 0; i < hiveCols.length; i++) {
      if (indexSet.contains(hiveCols[i].trim())) {
        if (sb.length() > 0) {
          sb.append(",");
        }
        sb.append(accCols[i].trim() + ":" + AccumuloIndexLexicoder.getRawType(hiveTypes[i]));
      }
    }

    return sb.toString();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveAccumuloTableInputFormat.class;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveAccumuloTableOutputFormat.class;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    boolean isExternal = isExternalTable(table);
    if (table.getSd().getLocation() != null) {
      throw new MetaException("Location can't be specified for Accumulo");
    }

    Map<String, String> serdeParams = table.getSd().getSerdeInfo().getParameters();
    String columnMapping = serdeParams.get(AccumuloSerDeParameters.COLUMN_MAPPINGS);
    if (columnMapping == null) {
      throw new MetaException(AccumuloSerDeParameters.COLUMN_MAPPINGS
          + " missing from SERDEPROPERTIES");
    }

    try {
      String tblName = getTableName(table);
      Connector connector = connectionParams.getConnector();
      TableOperations tableOpts = connector.tableOperations();

      // Attempt to create the table, taking EXTERNAL into consideration
      if (!tableOpts.exists(tblName)) {
        if (!isExternal) {
          tableOpts.create(tblName);
        } else {
          throw new MetaException("Accumulo table " + tblName
              + " doesn't exist even though declared external");
        }
      } else {
        if (!isExternal) {
          throw new MetaException("Table " + tblName
              + " already exists in Accumulo. Use CREATE EXTERNAL TABLE to register with Hive.");
        }
      }

      String idxTable = getIndexTableName(table);

      if (idxTable != null && !idxTable.isEmpty()) {

        // create the index table if it does not exist
        if (!tableOpts.exists(idxTable)) {
          tableOpts.create(idxTable);
        }
      }
    } catch (AccumuloSecurityException e) {
      throw new MetaException(StringUtils.stringifyException(e));
    } catch (TableExistsException e) {
      throw new MetaException(StringUtils.stringifyException(e));
    } catch (AccumuloException e) {
      throw new MetaException(StringUtils.stringifyException(e));
    }
  }

  protected boolean isExternalTable(Table table) {
    return MetaStoreUtils.isExternalTable(table);
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // Same as commitDropTable where we always delete the data (accumulo table)
    commitDropTable(table, true);
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // do nothing
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    String tblName = getTableName(table);
    if (!isExternalTable(table)) {
      try {
        if (deleteData) {
          TableOperations tblOpts = connectionParams.getConnector().tableOperations();
          if (tblOpts.exists(tblName)) {
            tblOpts.delete(tblName);
          }
        }
      } catch (AccumuloException e) {
        throw new MetaException(StringUtils.stringifyException(e));
      } catch (AccumuloSecurityException e) {
        throw new MetaException(StringUtils.stringifyException(e));
      } catch (TableNotFoundException e) {
        throw new MetaException(StringUtils.stringifyException(e));
      }
    }
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // do nothing
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // do nothing
  }

  @Override
  public DecomposedPredicate decomposePredicate(JobConf conf, Deserializer deserializer,
      ExprNodeDesc desc) {
    if (!(deserializer instanceof AccumuloSerDe)) {
      throw new RuntimeException("Expected an AccumuloSerDe but got "
          + deserializer.getClass().getName());
    }

    AccumuloSerDe serDe = (AccumuloSerDe) deserializer;
    if (serDe.getIteratorPushdown()) {
      return predicateHandler.decompose(conf, desc);
    } else {
      LOG.info("Set to ignore Accumulo iterator pushdown, skipping predicate handler.");
      return null;
    }
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    helper.loadDependentJars(jobConf);

    Properties tblProperties = tableDesc.getProperties();
    AccumuloSerDeParameters serDeParams = null;
    try {
      serDeParams =
          new AccumuloSerDeParameters(jobConf, tblProperties, AccumuloSerDe.class.getName());
    } catch (SerDeException e) {
      LOG.error("Could not instantiate AccumuloSerDeParameters", e);
      return;
    }

    try {
      serDeParams.getRowIdFactory().addDependencyJars(jobConf);
    } catch (IOException e) {
      LOG.error("Could not add necessary dependencies for "
          + serDeParams.getRowIdFactory().getClass(), e);
    }

    // When Kerberos is enabled, we have to add the Accumulo delegation token to the
    // Job so that it gets passed down to the YARN/Tez task.
    if (connectionParams.useSasl()) {
      try {
        // Open an accumulo connection
        Connector conn = connectionParams.getConnector();

        // Convert the Accumulo token in a Hadoop token
        Token<? extends TokenIdentifier> accumuloToken = helper.setConnectorInfoForInputAndOutput(connectionParams, conn, jobConf);

        LOG.debug("Adding Hadoop Token for Accumulo to Job's Credentials");

        // Add the Hadoop token to the JobConf
        helper.mergeTokenIntoJobConf(jobConf, accumuloToken);
        LOG.debug("All job tokens: " + jobConf.getCredentials().getAllTokens());
      } catch (Exception e) {
        throw new RuntimeException("Failed to obtain DelegationToken for "
            + connectionParams.getAccumuloUserName(), e);
      }
    }
  }
}
