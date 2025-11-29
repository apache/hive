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

public enum DatabaseType {
  // Default quote is "
  H2,
  DB2,
  DERBY,
  ORACLE,
  POSTGRES,
  JETHRO_DATA,
  MSSQL,
  
  // Special quote cases
  MYSQL("`"),
  MARIADB("`"),
  HIVE("`"),
  
  // METASTORE will be resolved to another type
  METASTORE(null, null);
  
  // Keeping start and end quote separate to allow for future DBs 
  // that may have different start and end quotes
  private final String startQuote;
  private final String endQuote;
  
  DatabaseType() {
    this("\"", "\"");
  }
  
  DatabaseType(String quote) {
    this(quote, quote);
  }
  
  DatabaseType(String startQuote, String endQuote) {
    this.startQuote = startQuote;
    this.endQuote = endQuote;
  }

  /**
   * Helper to safely get the DatabaseType from a string.
   * 
   * @param dbType The string from configuration properties.
   * @return The matching DatabaseType.
   * @throws IllegalArgumentException if the dbType is null or not a valid type.
   */
  public static DatabaseType from(String dbType) {
    if (dbType == null) {
      throw new IllegalArgumentException("Database type string cannot be null");
    }
    // METASTORE must be handled before valueOf
    if (METASTORE.name().equalsIgnoreCase(dbType)) {
      return METASTORE;
    }
    try {
      return valueOf(dbType.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid database type: " + dbType, e);
    }
  }

  public String getStartQuote() {
    return startQuote;
  }

  public String getEndQuote() {
    return endQuote;
  }
}
