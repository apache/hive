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
package org.apache.hadoop.hive.metastore.datasource;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

/**
 * DataSourceProvider for the BoneCP connection pool.
 */
public class BoneCPDataSourceProvider implements DataSourceProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BoneCPDataSourceProvider.class);

  public static final String BONECP = "bonecp";
  private static final String CONNECTION_TIMEOUT_PROPERTY= "bonecp.connectionTimeoutInMs";
  private static final String PARTITION_COUNT_PROPERTY= "bonecp.partitionCount";

  @Override
  public DataSource create(Configuration hdpConfig) throws SQLException {

    LOG.debug("Creating BoneCP connection pool for the MetaStore");

    String driverUrl = DataSourceProvider.getMetastoreJdbcDriverUrl(hdpConfig);
    String user = DataSourceProvider.getMetastoreJdbcUser(hdpConfig);
    String passwd = DataSourceProvider.getMetastoreJdbcPasswd(hdpConfig);
    int maxPoolSize = MetastoreConf.getIntVar(hdpConfig,
        MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS);

    Properties properties = DataSourceProvider.getPrefixedProperties(hdpConfig, BONECP);
    long connectionTimeout = hdpConfig.getLong(CONNECTION_TIMEOUT_PROPERTY, 30000L);
    String partitionCount = properties.getProperty(PARTITION_COUNT_PROPERTY, "1");

    BoneCPConfig config = null;
    try {
      config = new BoneCPConfig(properties);
    } catch (Exception e) {
      throw new SQLException("Cannot create BoneCP configuration: ", e);
    }
    config.setJdbcUrl(driverUrl);
    //if we are waiting for connection for a long time, something is really wrong
    //better raise an error than hang forever
    //see DefaultConnectionStrategy.getConnectionInternal()
    config.setConnectionTimeoutInMs(connectionTimeout);
    config.setMaxConnectionsPerPartition(maxPoolSize);
    config.setPartitionCount(Integer.parseInt(partitionCount));
    config.setUser(user);
    config.setPassword(passwd);
    return new BoneCPDataSource(config);
  }

  @Override
  public boolean mayReturnClosedConnection() {
    // See HIVE-11915 for details
    return true;
  }

  @Override
  public boolean supports(Configuration configuration) {
    String poolingType = MetastoreConf.getVar(configuration,
            MetastoreConf.ConfVars.CONNECTION_POOLING_TYPE).toLowerCase();
    if (BONECP.equals(poolingType)) {
      int boneCpPropsNr = DataSourceProvider.getPrefixedProperties(configuration, BONECP).size();
      LOG.debug("Found " + boneCpPropsNr + " nr. of bonecp specific configurations");
      return boneCpPropsNr > 0;
    }
    LOG.debug("Configuration requested " + poolingType + " pooling, BoneCpDSProvider exiting");
    return false;
  }
}
