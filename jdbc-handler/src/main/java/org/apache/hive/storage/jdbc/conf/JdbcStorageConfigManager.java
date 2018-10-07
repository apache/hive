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
package org.apache.hive.storage.jdbc.conf;

import java.io.IOException;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hadoop.conf.Configuration;

import org.apache.hive.storage.jdbc.QueryConditionBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Main configuration handler class
 */
public class JdbcStorageConfigManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcStorageConfigManager.class);
  public static final String CONFIG_USERNAME = Constants.JDBC_USERNAME;
  public static final String CONFIG_PWD = Constants.JDBC_PASSWORD;
  public static final String CONFIG_PWD_KEYSTORE = Constants.JDBC_KEYSTORE;
  public static final String CONFIG_PWD_KEY = Constants.JDBC_KEY;
  private static final EnumSet<JdbcStorageConfig> DEFAULT_REQUIRED_PROPERTIES =
    EnumSet.of(JdbcStorageConfig.DATABASE_TYPE,
               JdbcStorageConfig.JDBC_URL,
               JdbcStorageConfig.JDBC_DRIVER_CLASS);

  private static final EnumSet<JdbcStorageConfig> METASTORE_REQUIRED_PROPERTIES =
    EnumSet.of(JdbcStorageConfig.DATABASE_TYPE,
               JdbcStorageConfig.QUERY);

  private JdbcStorageConfigManager() {
  }

  public static void copyConfigurationToJob(Properties props, Map<String, String> jobProps)
    throws HiveException, IOException {
    checkRequiredPropertiesAreDefined(props);
    resolveMetadata(props);
    for (Entry<Object, Object> entry : props.entrySet()) {
      if (!String.valueOf(entry.getKey()).equals(CONFIG_PWD) &&
          !String.valueOf(entry.getKey()).equals(CONFIG_PWD_KEYSTORE) &&
          !String.valueOf(entry.getKey()).equals(CONFIG_PWD_KEY)) {
        jobProps.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
      }
    }
  }

  public static void copySecretsToJob(Properties props, Map<String, String> jobSecrets)
    throws HiveException, IOException {
    checkRequiredPropertiesAreDefined(props);
    resolveMetadata(props);
    String passwd = props.getProperty(CONFIG_PWD);
    if (passwd == null) {
      String keystore = props.getProperty(CONFIG_PWD_KEYSTORE);
      String key = props.getProperty(CONFIG_PWD_KEY);
      passwd = Utilities.getPasswdFromKeystore(keystore, key);
    }
    if (passwd != null) {
      jobSecrets.put(CONFIG_PWD, passwd);
    }
  }

  public static Configuration convertPropertiesToConfiguration(Properties props)
    throws HiveException, IOException {
    checkRequiredPropertiesAreDefined(props);
    resolveMetadata(props);
    Configuration conf = new Configuration();

    for (Entry<Object, Object> entry : props.entrySet()) {
      conf.set(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
    }

    return conf;
  }

  private static void checkRequiredPropertiesAreDefined(Properties props) {
    DatabaseType dbType = null;

    try {
      String dbTypeName = props.getProperty(JdbcStorageConfig.DATABASE_TYPE.getPropertyName());
      dbType = DatabaseType.valueOf(dbTypeName);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unknown database type.", e);
    }

    for (JdbcStorageConfig configKey : (DatabaseType.METASTORE.equals(dbType)
            ? METASTORE_REQUIRED_PROPERTIES : DEFAULT_REQUIRED_PROPERTIES)) {
      String propertyKey = configKey.getPropertyName();
      if ((props == null) || (!props.containsKey(propertyKey)) || (isEmptyString(props.getProperty(propertyKey)))) {
        throw new IllegalArgumentException("Property " + propertyKey + " is required.");
      }
    }

    CustomConfigManager configManager = CustomConfigManagerFactory.getCustomConfigManagerFor(dbType);
    configManager.checkRequiredProperties(props);
  }


  public static String getConfigValue(JdbcStorageConfig key, Configuration config) {
    return config.get(key.getPropertyName());
  }


  public static String getQueryToExecute(Configuration config) {
    String query = config.get(Constants.JDBC_QUERY);
    if (query != null) {
      // Already defined query, we return it
      return query;
    }

    // We generate query as select *
    String tableName = config.get(JdbcStorageConfig.TABLE.getPropertyName());
    query = "select * from " + tableName;

    String hiveFilterCondition = QueryConditionBuilder.getInstance().buildCondition(config);
    if ((hiveFilterCondition != null) && (!hiveFilterCondition.trim().isEmpty())) {
      query = query + " WHERE " + hiveFilterCondition;
    }

    return query;
  }


  private static boolean isEmptyString(String value) {
    return ((value == null) || (value.trim().isEmpty()));
  }

  private static void resolveMetadata(Properties props) throws HiveException, IOException {
    DatabaseType dbType = DatabaseType.valueOf(
      props.getProperty(JdbcStorageConfig.DATABASE_TYPE.getPropertyName()));

    LOGGER.debug("Resolving db type: {}", dbType.toString());

    if (dbType == DatabaseType.METASTORE) {
      HiveConf hconf = Hive.get().getConf();
      props.setProperty(JdbcStorageConfig.JDBC_URL.getPropertyName(),
          getMetastoreConnectionURL(hconf));
      props.setProperty(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName(),
          getMetastoreDriver(hconf));

      String user = getMetastoreJdbcUser(hconf);
      if (user != null) {
        props.setProperty(CONFIG_USERNAME, user);
      }

      String pwd = getMetastoreJdbcPasswd(hconf);
      if (pwd != null) {
        props.setProperty(CONFIG_PWD, pwd);
      }
      props.setProperty(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(),
          getMetastoreDatabaseType(hconf));
    }
  }

  private static String getMetastoreDatabaseType(HiveConf conf) {
    return conf.getVar(HiveConf.ConfVars.METASTOREDBTYPE);
  }

  private static String getMetastoreConnectionURL(HiveConf conf) {
    return conf.getVar(HiveConf.ConfVars.METASTORECONNECTURLKEY);
  }

  private static String getMetastoreDriver(HiveConf conf) {
    return conf.getVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER);
  }

  private static String getMetastoreJdbcUser(HiveConf conf) {
    return conf.getVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME);
  }

  private static String getMetastoreJdbcPasswd(HiveConf conf) throws IOException {
    return ShimLoader.getHadoopShims().getPassword(conf,
        HiveConf.ConfVars.METASTOREPWD.varname);
  }
}
