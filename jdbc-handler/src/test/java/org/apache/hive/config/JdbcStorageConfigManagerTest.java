/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.config;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import org.apache.hive.storage.jdbc.conf.DatabaseType;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JdbcStorageConfigManagerTest {

  @Test
  public void testWithAllRequiredSettingsDefined() {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), DatabaseType.MYSQL.toString());
    props.put(JdbcStorageConfig.JDBC_URL.getPropertyName(), "jdbc://localhost:3306/hive");
    props.put(JdbcStorageConfig.QUERY.getPropertyName(), "SELECT col1,col2,col3 FROM sometable");
    props.put(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName(), "com.mysql.jdbc.Driver");

    Map<String, String> jobMap = new HashMap<String, String>();
    JdbcStorageConfigManager.copyConfigurationToJob(props, jobMap);

    assertThat(jobMap, is(notNullValue()));
    assertThat(jobMap.size(), is(equalTo(4)));
    assertThat(jobMap.get(JdbcStorageConfig.DATABASE_TYPE.getPropertyName()), is(equalTo("MYSQL")));
    assertThat(jobMap.get(JdbcStorageConfig.JDBC_URL.getPropertyName()), is(equalTo("jdbc://localhost:3306/hive")));
    assertThat(jobMap.get(JdbcStorageConfig.QUERY.getPropertyName()),
        is(equalTo("SELECT col1,col2,col3 FROM sometable")));
  }


  @Test(expected = IllegalArgumentException.class)
  public void testWithJdbcUrlMissing() {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), DatabaseType.MYSQL.toString());
    props.put(JdbcStorageConfig.QUERY.getPropertyName(), "SELECT col1,col2,col3 FROM sometable");

    Map<String, String> jobMap = new HashMap<String, String>();
    JdbcStorageConfigManager.copyConfigurationToJob(props, jobMap);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testWithDatabaseTypeMissing() {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.JDBC_URL.getPropertyName(), "jdbc://localhost:3306/hive");
    props.put(JdbcStorageConfig.QUERY.getPropertyName(), "SELECT col1,col2,col3 FROM sometable");

    Map<String, String> jobMap = new HashMap<String, String>();
    JdbcStorageConfigManager.copyConfigurationToJob(props, jobMap);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testWithUnknownDatabaseType() {
    Properties props = new Properties();
    props.put(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), "Postgres");
    props.put(JdbcStorageConfig.JDBC_URL.getPropertyName(), "jdbc://localhost:3306/hive");
    props.put(JdbcStorageConfig.QUERY.getPropertyName(), "SELECT col1,col2,col3 FROM sometable");

    Map<String, String> jobMap = new HashMap<String, String>();
    JdbcStorageConfigManager.copyConfigurationToJob(props, jobMap);
  }

}
