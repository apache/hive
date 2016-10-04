/**
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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;

/**
 * List of hive operations types.
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
public enum HiveOperationType {
  EXPLAIN,
  LOAD,
  EXPORT,
  IMPORT,
  CREATEDATABASE,
  DROPDATABASE,
  SWITCHDATABASE,
  LOCKDB,
  UNLOCKDB,
  DROPTABLE ,
  DESCTABLE,
  DESCFUNCTION,
  MSCK,
  ALTERTABLE_ADDCOLS,
  ALTERTABLE_REPLACECOLS,
  ALTERTABLE_RENAMECOL,
  ALTERTABLE_RENAMEPART,
  ALTERTABLE_RENAME,
  ALTERTABLE_DROPPARTS,
  ALTERTABLE_ADDPARTS,
  ALTERTABLE_TOUCH,
  ALTERTABLE_ARCHIVE,
  ALTERTABLE_UNARCHIVE,
  ALTERTABLE_PROPERTIES,
  ALTERTABLE_SERIALIZER,
  ALTERTABLE_PARTCOLTYPE,
  ALTERTABLE_DROPCONSTRAINT,
  ALTERTABLE_ADDCONSTRAINT,
  ALTERPARTITION_SERIALIZER,
  ALTERTABLE_SERDEPROPERTIES,
  ALTERPARTITION_SERDEPROPERTIES,
  ALTERTABLE_CLUSTER_SORT,
  ANALYZE_TABLE,
  CACHE_METADATA,
  ALTERTABLE_BUCKETNUM,
  ALTERPARTITION_BUCKETNUM,
  ALTERTABLE_UPDATETABLESTATS,
  ALTERTABLE_UPDATEPARTSTATS,
  SHOWDATABASES,
  SHOWTABLES,
  SHOWCOLUMNS,
  SHOW_TABLESTATUS,
  SHOW_TBLPROPERTIES,
  SHOW_CREATEDATABASE,
  SHOW_CREATETABLE,
  SHOWFUNCTIONS,
  SHOWINDEXES,
  SHOWPARTITIONS,
  SHOWLOCKS,
  SHOWCONF,
  SHOWVIEWS,
  CREATEFUNCTION,
  DROPFUNCTION,
  RELOADFUNCTION,
  CREATEMACRO,
  DROPMACRO,
  CREATEVIEW,
  CREATE_MATERIALIZED_VIEW,
  DROPVIEW,
  DROP_MATERIALIZED_VIEW,
  CREATEINDEX,
  DROPINDEX,
  ALTERINDEX_REBUILD,
  ALTERVIEW_PROPERTIES,
  DROPVIEW_PROPERTIES,
  LOCKTABLE,
  UNLOCKTABLE,
  CREATEROLE,
  DROPROLE,
  GRANT_PRIVILEGE,
  REVOKE_PRIVILEGE,
  SHOW_GRANT,
  GRANT_ROLE,
  REVOKE_ROLE,
  SHOW_ROLES,
  SHOW_ROLE_GRANT,
  SHOW_ROLE_PRINCIPALS,
  ALTERTABLE_PROTECTMODE,
  ALTERPARTITION_PROTECTMODE,
  ALTERTABLE_FILEFORMAT,
  ALTERPARTITION_FILEFORMAT,
  ALTERTABLE_LOCATION,
  ALTERPARTITION_LOCATION,
  CREATETABLE,
  TRUNCATETABLE,
  CREATETABLE_AS_SELECT,
  QUERY,
  ALTERINDEX_PROPS,
  ALTERDATABASE,
  ALTERDATABASE_OWNER,
  DESCDATABASE,
  ALTERTABLE_MERGEFILES,
  ALTERPARTITION_MERGEFILES,
  ALTERTABLE_SKEWED,
  ALTERTBLPART_SKEWED_LOCATION,
  ALTERVIEW_RENAME,
  ALTERVIEW_AS,
  ALTERTABLE_COMPACT,
  SHOW_COMPACTIONS,
  SHOW_TRANSACTIONS,
  ABORT_TRANSACTIONS,
  // ==== Hive command operation types starts here ==== //
  SET,
  RESET,
  DFS,
  ADD,
  DELETE,
  COMPILE,
  START_TRANSACTION,
  COMMIT,
  ROLLBACK,
  SET_AUTOCOMMIT,
  ALTERTABLE_EXCHANGEPARTITION,
  // ==== Hive command operations ends here ==== //

  // ==== HiveServer2 metadata api types start here ==== //
  // these corresponds to various java.sql.DatabaseMetaData calls.
  GET_CATALOGS, // DatabaseMetaData.getCatalogs()  catalogs are actually not supported in
                // hive, so this is a no-op

  GET_COLUMNS, // getColumns(String catalog, String schemaPattern, String
               // tableNamePattern, String columnNamePattern)

  GET_FUNCTIONS, // getFunctions(String catalog, String schemaPattern, String functionNamePattern)
  GET_SCHEMAS, // getSchemas()
  GET_TABLES, // getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
  GET_TABLETYPES,// getTableTypes()
  GET_TYPEINFO // getTypeInfo()
  // ==== HiveServer2 metadata api types ends here ==== //

}
