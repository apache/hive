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

import org.apache.hadoop.hive.conf.Constants;

public enum JdbcStorageConfig {
  DATABASE_TYPE(Constants.JDBC_DATABASE_TYPE, true),
  JDBC_URL(Constants.JDBC_URL, true),
  JDBC_DRIVER_CLASS(Constants.JDBC_DRIVER, true),
  QUERY(Constants.JDBC_QUERY, false),
  TABLE(Constants.JDBC_TABLE, false),
  JDBC_FETCH_SIZE(Constants.JDBC_CONFIG_PREFIX + ".jdbc.fetch.size", false),
  COLUMN_MAPPING(Constants.JDBC_CONFIG_PREFIX + ".column.mapping", false);

  private String propertyName;
  private boolean required = false;


  JdbcStorageConfig(String propertyName, boolean required) {
    this.propertyName = propertyName;
    this.required = required;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public boolean isRequired() {
    return required;
  }

}
