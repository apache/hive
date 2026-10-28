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
package org.apache.hadoop.hive.ql.ddl;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * DDL-time checks for JDBC storage-handler table properties.
 */
public final class JdbcStorageHandlerDdlUtils {

  private static final String JDBC_STORAGE_HANDLER =
      "org.apache.hive.storage.jdbc.JdbcStorageHandler";
  private static final String METASTORE_DATABASE_TYPE = "METASTORE";
  private static final String SYS_DATABASE_NAME = "sys";

  private JdbcStorageHandlerDdlUtils() {
  }

  public static void validateMetastoreDatabaseTypeProperty(HiveConf conf, String storageHandler,
      String databaseName, Map<String, String> tblProps) throws SemanticException {
    if (!JDBC_STORAGE_HANDLER.equals(storageHandler) || tblProps == null) {
      return;
    }
    String dbType = tblProps.get(Constants.JDBC_DATABASE_TYPE);
    if (!METASTORE_DATABASE_TYPE.equalsIgnoreCase(dbType)) {
      return;
    }
    if (StringUtils.isNotBlank(databaseName)
        && SYS_DATABASE_NAME.equalsIgnoreCase(databaseName.trim())) {
      return;
    }
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STORAGE_JDBC_ALLOW_METASTORE_DATABASE_TYPE)) {
      return;
    }
    throw new SemanticException(
        "hive.sql.database.type=METASTORE is only permitted for tables in the SYS database");
  }
}
