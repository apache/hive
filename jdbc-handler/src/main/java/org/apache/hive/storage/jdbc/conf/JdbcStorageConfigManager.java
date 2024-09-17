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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Function;

/**
 * Main configuration handler class
 */
public class JdbcStorageConfigManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcStorageConfigManager.class);
  public static final String CONFIG_USERNAME = Constants.JDBC_USERNAME;
  public static final String CONFIG_PWD = Constants.JDBC_PASSWORD;
  public static final String CONFIG_PWD_KEYSTORE = Constants.JDBC_KEYSTORE;
  public static final String CONFIG_PWD_KEY = Constants.JDBC_KEY;
  public static final String CONFIG_PWD_URI = Constants.JDBC_PASSWORD_URI;
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
      String key = String.valueOf(entry.getKey());
      if (!key.equals(CONFIG_PWD) &&
          !key.equals(CONFIG_PWD_KEYSTORE) &&
          !key.equals(CONFIG_PWD_KEY) &&
          !key.equals(CONFIG_PWD_URI)) {
        jobProps.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
      }
    }
  }

  private static int countNonNull(String ... values) {
    int count = 0;
    for (String str : values) {
      if (str != null) {
        count++;
      }
    }
    return count;
  }

  public static String getPasswordFromProperties(Properties properties, Function<String, String> keyTransform)
      throws HiveException, IOException {
    String passwd = properties.getProperty(keyTransform.apply(CONFIG_PWD));
    String keystore = properties.getProperty(keyTransform.apply(CONFIG_PWD_KEYSTORE));
    String uri = properties.getProperty(keyTransform.apply(CONFIG_PWD_URI));
    if (countNonNull(passwd, keystore, uri) > 1) {
      // In tez, when the job conf is copied there is a code path in HiveInputFormat where all the table properties
      // are copied and the password is copied from the job credentials, so its possible to have 2 of them set.
      // For now ignore this and print a warning message, we should fix so that the above code is used instead.
      LOGGER.warn("Only one of " + CONFIG_PWD + ", " + CONFIG_PWD_KEYSTORE + ", " + CONFIG_PWD_URI + " can be set");
      // throw new HiveException(
      //    "Only one of " + CONFIG_PWD + ", " + CONFIG_PWD_KEYSTORE + ", " + CONFIG_PWD_URI + " can be set");
    }
    if (passwd == null && keystore != null) {
      String key = properties.getProperty(keyTransform.apply(CONFIG_PWD_KEY));
      passwd = Utilities.getPasswdFromKeystore(keystore, key);
    }
    if (passwd == null && uri != null) {
      try {
        passwd = Utilities.getPasswdFromUri(uri);
      } catch (URISyntaxException e) {
        // Should I include the uri in the exception? Suppressing for now, since it may have sensitive info.
        throw new HiveException("Invalid password uri specified", e);
      }
    }
    return passwd;
  }

  public static void copySecretsToJob(Properties props, Map<String, String> jobSecrets)
      throws HiveException, IOException {
    checkRequiredPropertiesAreDefined(props);
    resolveMetadata(props);
    String passwd = getPasswordFromProperties(props, Function.identity());
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
    return conf.getVar(HiveConf.ConfVars.METASTORE_DB_TYPE);
  }

  private static String getMetastoreConnectionURL(HiveConf conf) {
    return conf.getVar(HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY);
  }

  private static String getMetastoreDriver(HiveConf conf) {
    return conf.getVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER);
  }

  private static String getMetastoreJdbcUser(HiveConf conf) {
    return conf.getVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME);
  }

  private static String getMetastoreJdbcPasswd(HiveConf conf) throws IOException {
    return ShimLoader.getHadoopShims().getPassword(conf,
        HiveConf.ConfVars.METASTORE_PWD.varname);
  }
}
