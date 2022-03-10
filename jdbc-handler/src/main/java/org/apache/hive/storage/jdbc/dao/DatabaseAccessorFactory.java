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
package org.apache.hive.storage.jdbc.dao;

import org.apache.hadoop.conf.Configuration;

import org.apache.hive.storage.jdbc.conf.DatabaseType;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;

/**
 * Factory for creating the correct DatabaseAccessor class for the job
 */
public class DatabaseAccessorFactory {

  private DatabaseAccessorFactory() {
  }


  public static DatabaseAccessor getAccessor(DatabaseType dbType, String driverClass) {

    DatabaseAccessor accessor = null;
    switch (dbType) {
    case MYSQL:
      accessor = new MySqlDatabaseAccessor();
      break;

    case JETHRO_DATA:
      accessor = new JethroDatabaseAccessor();
      break;

    case POSTGRES:
      accessor = new PostgresDatabaseAccessor();
      break;

    case ORACLE:
      accessor = new OracleDatabaseAccessor();
      break;

    case MSSQL:
      accessor = new MsSqlDatabaseAccessor();
      break;

    case DB2:
      accessor = new DB2DatabaseAccessor();
      break;

    case HIVE:
      accessor = new HiveDatabaseAccessor();
      break;

    case DERBY:
      accessor = new DerbyDatabaseAccessor();
      break;

    case METASTORE:
      // For metastore, we infer the accessor from the jdbc driver string.
      // TODO: We could also make a call to get the metadata from the connection
      //       so we could obtain the database product. However, it seems an
      //       overkill given that the metastore supported RDBMSs is a
      //       well-defined set.
      if (driverClass.contains("MYSQL")) {
        accessor = new MySqlDatabaseAccessor();
      } else if (driverClass.contains("POSTGRESQL")) {
        accessor = new PostgresDatabaseAccessor();
      } else if (driverClass.contains("ORACLE")) {
        accessor = new OracleDatabaseAccessor();
      } else if (driverClass.contains("SQLSERVER")) {
        accessor = new MsSqlDatabaseAccessor();
      } else {
        // default
        accessor = new GenericJdbcDatabaseAccessor();
      }
      break;

    default:
      accessor = new GenericJdbcDatabaseAccessor();
      break;
    }

    return accessor;
  }


  public static DatabaseAccessor getAccessor(Configuration conf) {
    DatabaseType dbType = DatabaseType.valueOf(
        conf.get(JdbcStorageConfig.DATABASE_TYPE.getPropertyName()).toUpperCase());
    String driverClass =
        conf.get(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName()).toUpperCase();
    return getAccessor(dbType, driverClass);
  }

}
