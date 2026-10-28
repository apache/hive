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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.storage.jdbc.conf;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.fail;

public class TestJdbcStorageConfigManager {

  @Test
  public void testWithAllRequiredSettingsDefined() throws Exception {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), DatabaseType.MYSQL.toString());
    props.put(JdbcStorageConfig.JDBC_URL.getPropertyName(), "jdbc://localhost:3306/hive");
    props.put(JdbcStorageConfig.QUERY.getPropertyName(), "SELECT col1,col2,col3 FROM sometable");
    props.put(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName(), "com.mysql.jdbc.Driver");

    Map<String, String> jobMap = new HashMap<>();
    JdbcStorageConfigManager.copyConfigurationToJob(props, jobMap);

    assertThat(jobMap, is(notNullValue()));
    assertThat(jobMap.size(), is(equalTo(4)));
    assertThat(jobMap.get(JdbcStorageConfig.DATABASE_TYPE.getPropertyName()), is(equalTo("MYSQL")));
    assertThat(jobMap.get(JdbcStorageConfig.JDBC_URL.getPropertyName()), is(equalTo("jdbc://localhost:3306/hive")));
    assertThat(jobMap.get(JdbcStorageConfig.QUERY.getPropertyName()),
        is(equalTo("SELECT col1,col2,col3 FROM sometable")));
  }


  // since metastore connections don't require the url, this is allowable.
  @Ignore @Test(expected = IllegalArgumentException.class)
  public void testWithJdbcUrlMissing() throws Exception {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), DatabaseType.MYSQL.toString());
    props.put(JdbcStorageConfig.QUERY.getPropertyName(), "SELECT col1,col2,col3 FROM sometable");

    Map<String, String> jobMap = new HashMap<>();
    JdbcStorageConfigManager.copyConfigurationToJob(props, jobMap);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testWithDatabaseTypeMissing() throws Exception {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.JDBC_URL.getPropertyName(), "jdbc://localhost:3306/hive");
    props.put(JdbcStorageConfig.QUERY.getPropertyName(), "SELECT col1,col2,col3 FROM sometable");

    Map<String, String> jobMap = new HashMap<>();
    JdbcStorageConfigManager.copyConfigurationToJob(props, jobMap);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testWithUnknownDatabaseType() throws Exception {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), "Postgres");
    props.put(JdbcStorageConfig.JDBC_URL.getPropertyName(), "jdbc://localhost:3306/hive");
    props.put(JdbcStorageConfig.QUERY.getPropertyName(), "SELECT col1,col2,col3 FROM sometable");

    Map<String, String> jobMap = new HashMap<>();
    JdbcStorageConfigManager.copyConfigurationToJob(props, jobMap);
  }

  @Test
  public void testMetastoreDatabaseTypeRejectedOutsideSys() throws Exception {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), DatabaseType.METASTORE.toString());
    props.put(JdbcStorageConfig.QUERY.getPropertyName(), "SELECT 1");
    try {
      JdbcStorageConfigManager.assertMetastoreDatabaseTypeAllowed(props, "attacker_db", new HiveConf());
      fail("Expected HiveException for METASTORE type outside SYS");
    } catch (HiveException expected) {
      assertThat(expected.getMessage(), is(equalTo(
          "hive.sql.database.type=METASTORE is only permitted for tables in the SYS database")));
    }
  }

  @Test
  public void testMetastoreDatabaseTypeAllowedInSys() throws Exception {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), DatabaseType.METASTORE.toString());
    props.put(JdbcStorageConfig.QUERY.getPropertyName(), "SELECT 1");
    JdbcStorageConfigManager.assertMetastoreDatabaseTypeAllowed(props, "sys", new HiveConf());
  }

  @Test
  public void testMetastoreDatabaseTypeAllowedFromTablePropertiesDbName() throws Exception {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), DatabaseType.METASTORE.toString());
    props.put(hive_metastoreConstants.META_TABLE_DB, "SYS");
    JdbcStorageConfigManager.assertMetastoreDatabaseTypeAllowed(props, null, new HiveConf());
  }

  @Test
  public void testMetastoreDatabaseTypeAllowedFromMetaTableName() throws Exception {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), DatabaseType.METASTORE.toString());
    props.put(hive_metastoreConstants.META_TABLE_NAME, "sys.dbs");
    JdbcStorageConfigManager.assertMetastoreDatabaseTypeAllowed(props, null, new HiveConf());
  }

  @Test
  public void testMetastoreDatabaseTypeRejectedWhenOnlySessionDbWouldBeSys() throws Exception {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), DatabaseType.METASTORE.toString());
    props.put(hive_metastoreConstants.META_TABLE_NAME, "attacker_db.exfil");
    try {
      JdbcStorageConfigManager.assertMetastoreDatabaseTypeAllowed(props, null, new HiveConf());
      fail("Expected HiveException for METASTORE table outside SYS");
    } catch (HiveException expected) {
      // expected
    }
  }

  @Test
  public void testExtractPassword() throws Exception {
    String prefix = "test.";
    String password = "my-super-secret";
    Properties props = new Properties();
    props.put(prefix + JdbcStorageConfigManager.CONFIG_PWD, password);
    props.put(prefix + JdbcStorageConfigManager.CONFIG_PWD_URI, "test:///random-stuff");

    String passwd = JdbcStorageConfigManager.getPasswordFromProperties(props, s -> prefix + s);
    assertThat(passwd, is(equalTo(password)));
  }
}
