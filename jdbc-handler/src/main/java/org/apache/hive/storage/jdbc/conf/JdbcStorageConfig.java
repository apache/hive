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

public enum JdbcStorageConfig {
  DATABASE_TYPE("database.type", true),
  JDBC_URL("jdbc.url", true),
  JDBC_DRIVER_CLASS("jdbc.driver", true),
  QUERY("query", true),
  JDBC_FETCH_SIZE("jdbc.fetch.size", false),
  COLUMN_MAPPING("column.mapping", false);

  private String propertyName;
  private boolean required = false;


  JdbcStorageConfig(String propertyName, boolean required) {
    this.propertyName = propertyName;
    this.required = required;
  }


  JdbcStorageConfig(String propertyName) {
    this.propertyName = propertyName;
  }


  public String getPropertyName() {
    return JdbcStorageConfigManager.CONFIG_PREFIX + "." + propertyName;
  }


  public boolean isRequired() {
    return required;
  }

}
