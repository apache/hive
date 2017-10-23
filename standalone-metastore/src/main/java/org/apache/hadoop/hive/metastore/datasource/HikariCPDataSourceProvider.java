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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

/**
 * DataSourceProvider for the HikariCP connection pool.
 */
public class HikariCPDataSourceProvider implements DataSourceProvider {

  private static final Logger LOG = LoggerFactory.getLogger(HikariCPDataSourceProvider.class);

  public static final String HIKARI = "hikari";
  private static final String CONNECTION_TIMEOUT_PROPERTY= "hikari.connectionTimeout";

  @Override
  public DataSource create(Configuration hdpConfig) throws SQLException {

    LOG.debug("Creating Hikari connection pool for the MetaStore");

    String driverUrl = DataSourceProvider.getMetastoreJdbcDriverUrl(hdpConfig);
    String user = DataSourceProvider.getMetastoreJdbcUser(hdpConfig);
    String passwd = DataSourceProvider.getMetastoreJdbcPasswd(hdpConfig);
    int maxPoolSize = MetastoreConf.getIntVar(hdpConfig,
        MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS);

    Properties properties = replacePrefix(
        DataSourceProvider.getPrefixedProperties(hdpConfig, HIKARI));
    long connectionTimeout = hdpConfig.getLong(CONNECTION_TIMEOUT_PROPERTY, 30000L);
    HikariConfig config = null;
    try {
      config = new HikariConfig(properties);
    } catch (Exception e) {
      throw new SQLException("Cannot create HikariCP configuration: ", e);
    }
    config.setMaximumPoolSize(maxPoolSize);
    config.setJdbcUrl(driverUrl);
    config.setUsername(user);
    config.setPassword(passwd);
    //https://github.com/brettwooldridge/HikariCP
    config.setConnectionTimeout(connectionTimeout);
    return new HikariDataSource(config);
  }

  @Override
  public boolean mayReturnClosedConnection() {
    // Only BoneCP should return true
    return false;
  }

  @Override
  public boolean supports(Configuration configuration) {
    String poolingType = MetastoreConf.getVar(configuration,
            MetastoreConf.ConfVars.CONNECTION_POOLING_TYPE).toLowerCase();
    if (HIKARI.equals(poolingType)) {
      int hikariPropsNr = DataSourceProvider.getPrefixedProperties(configuration, HIKARI).size();
      LOG.debug("Found " + hikariPropsNr + " nr. of hikari specific configurations");
      return hikariPropsNr > 0;
    }
    LOG.debug("Configuration requested " + poolingType + " pooling, HikariCpDSProvider exiting");
    return false;
  }

  private Properties replacePrefix(Properties props) {
    Properties newProps = new Properties();
    props.forEach((key,value) ->
        newProps.put(key.toString().replaceFirst(HIKARI + ".", ""), value));
    return newProps;
  }
}
