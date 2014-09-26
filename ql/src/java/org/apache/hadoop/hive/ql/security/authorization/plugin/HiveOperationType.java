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
  ALTERPARTITION_SERIALIZER,
  ALTERTABLE_SERDEPROPERTIES,
  ALTERPARTITION_SERDEPROPERTIES,
  ALTERTABLE_CLUSTER_SORT,
  ANALYZE_TABLE,
  ALTERTABLE_BUCKETNUM,
  ALTERPARTITION_BUCKETNUM,
  ALTERTABLE_UPDATETABLESTATS,
  ALTERTABLE_UPDATEPARTSTATS,
  SHOWDATABASES,
  SHOWTABLES,
  SHOWCOLUMNS,
  SHOW_TABLESTATUS,
  SHOW_TBLPROPERTIES,
  SHOW_CREATETABLE,
  SHOWFUNCTIONS,
  SHOWINDEXES,
  SHOWPARTITIONS,
  SHOWLOCKS,
  SHOWCONF,
  CREATEFUNCTION,
  DROPFUNCTION,
  CREATEMACRO,
  DROPMACRO,
  CREATEVIEW,
  DROPVIEW,
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
  SET,
  RESET,
  DFS,
  ADD,
  DELETE,
  COMPILE

}
