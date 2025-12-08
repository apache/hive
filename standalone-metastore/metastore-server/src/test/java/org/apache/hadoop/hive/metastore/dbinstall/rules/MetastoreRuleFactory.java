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
package org.apache.hadoop.hive.metastore.dbinstall.rules;

/**
 * A factory for creating a Metastore database rule for use in tests.
 */
public final class MetastoreRuleFactory {
  private MetastoreRuleFactory() {
    throw new IllegalStateException("Factory class should not be instantiated");
  }

  /**
   * Creates a new Metastore rule based on the provided database type.
   *
   * @param dbType the type of database (e.g., "mysql", "postgres", etc.)
   * @return a DatabaseRule instance for the specified database type
   * @throws IllegalArgumentException if the database type is unsupported
   */
  public static DatabaseRule create(String dbType) {
    return switch (dbType.toLowerCase()) {
      case "mysql" -> new Mysql();
      case "postgres" -> new Postgres();
      case "postgres.tpcds" -> new PostgresTPCDS();
      case "mariadb" -> new Mariadb();
      case "derby" -> new Derby();
      case "derby.clean" -> new Derby(true);
      case "mssql", "sqlserver" -> new Mssql();
      case "oracle" -> new Oracle();
      default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
    };
  }
}
