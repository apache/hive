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

import org.apache.hadoop.conf.Configuration;

import org.apache.hive.storage.jdbc.QueryConditionBuilder;

import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Main configuration handler class
 */
public class JdbcStorageConfigManager {

  public static final String CONFIG_PREFIX = "hive.sql";
  private static final EnumSet<JdbcStorageConfig> DEFAULT_REQUIRED_PROPERTIES =
    EnumSet.of(JdbcStorageConfig.DATABASE_TYPE,
        JdbcStorageConfig.JDBC_URL,
        JdbcStorageConfig.JDBC_DRIVER_CLASS,
        JdbcStorageConfig.QUERY);


  private JdbcStorageConfigManager() {
  }


  public static void copyConfigurationToJob(Properties props, Map<String, String> jobProps) {
    checkRequiredPropertiesAreDefined(props);
    for (Entry<Object, Object> entry : props.entrySet()) {
      jobProps.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
    }
  }


  public static Configuration convertPropertiesToConfiguration(Properties props) {
    checkRequiredPropertiesAreDefined(props);
    Configuration conf = new Configuration();

    for (Entry<Object, Object> entry : props.entrySet()) {
      conf.set(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
    }

    return conf;
  }


  private static void checkRequiredPropertiesAreDefined(Properties props) {
    for (JdbcStorageConfig configKey : DEFAULT_REQUIRED_PROPERTIES) {
      String propertyKey = configKey.getPropertyName();
      if ((props == null) || (!props.containsKey(propertyKey)) || (isEmptyString(props.getProperty(propertyKey)))) {
        throw new IllegalArgumentException("Property " + propertyKey + " is required.");
      }
    }

    DatabaseType dbType = DatabaseType.valueOf(props.getProperty(JdbcStorageConfig.DATABASE_TYPE.getPropertyName()));
    CustomConfigManager configManager = CustomConfigManagerFactory.getCustomConfigManagerFor(dbType);
    configManager.checkRequiredProperties(props);
  }


  public static String getConfigValue(JdbcStorageConfig key, Configuration config) {
    return config.get(key.getPropertyName());
  }


  public static String getQueryToExecute(Configuration config) {
    String query = config.get(JdbcStorageConfig.QUERY.getPropertyName());
    String hiveFilterCondition = QueryConditionBuilder.getInstance().buildCondition(config);
    if ((hiveFilterCondition != null) && (!hiveFilterCondition.trim().isEmpty())) {
      query = query + " WHERE " + hiveFilterCondition;
    }

    return query;
  }


  private static boolean isEmptyString(String value) {
    return ((value == null) || (value.trim().isEmpty()));
  }

}
