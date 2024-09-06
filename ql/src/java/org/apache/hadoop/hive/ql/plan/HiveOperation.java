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

package org.apache.hadoop.hive.ql.plan;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;

public enum HiveOperation {
  EXPLAIN("EXPLAIN", HiveParser.TOK_EXPLAIN, null, null),
  LOAD("LOAD", HiveParser.TOK_LOAD, null, new Privilege[]{Privilege.ALTER_DATA}),
  EXPORT("EXPORT", HiveParser.TOK_EXPORT, new Privilege[]{Privilege.SELECT}, null),
  IMPORT("IMPORT", HiveParser.TOK_IMPORT, null, new Privilege[]{Privilege.ALTER_METADATA, Privilege.ALTER_DATA}),
  REPLDUMP("REPLDUMP", HiveParser.TOK_REPL_DUMP, new Privilege[]{Privilege.ALL}, null),
  REPLLOAD("REPLLOAD", HiveParser.TOK_REPL_LOAD, null, new Privilege[]{Privilege.ALL}),
  REPLSTATUS("REPLSTATUS", HiveParser.TOK_REPL_STATUS, new Privilege[]{Privilege.SELECT}, null),
  CREATEDATABASE("CREATEDATABASE", HiveParser.TOK_CREATEDATABASE, null, new Privilege[]{Privilege.CREATE}),
  CREATEDATACONNECTOR("CREATEDATACONNECTOR", HiveParser.TOK_CREATEDATACONNECTOR, null, new Privilege[]{Privilege.CREATE}),
  DROPDATABASE("DROPDATABASE", HiveParser.TOK_DROPDATABASE, null, new Privilege[]{Privilege.DROP}),
  DROPDATACONNECTOR("DROPDATACONNECTOR", HiveParser.TOK_DROPDATACONNECTOR, null, new Privilege[]{Privilege.DROP}),
  SWITCHDATABASE("SWITCHDATABASE", HiveParser.TOK_SWITCHDATABASE, null, null, true, false),
  LOCKDB("LOCKDATABASE", HiveParser.TOK_LOCKDB, new Privilege[]{Privilege.LOCK}, null),
  UNLOCKDB("UNLOCKDATABASE", HiveParser.TOK_UNLOCKDB, new Privilege[]{Privilege.LOCK}, null),
  DROPTABLE ("DROPTABLE", HiveParser.TOK_DROPTABLE, null, new Privilege[]{Privilege.DROP}),
  DESCTABLE("DESCTABLE", HiveParser.TOK_DESCTABLE, null, null),
  DESCFUNCTION("DESCFUNCTION", HiveParser.TOK_DESCFUNCTION, null, null),
  MSCK("MSCK", HiveParser.TOK_MSCK, null, null),
  ALTERTABLE_ADDCOLS("ALTERTABLE_ADDCOLS", HiveParser.TOK_ALTERTABLE_ADDCOLS, new Privilege[]{Privilege.ALTER_METADATA},
      null),
  ALTERTABLE_REPLACECOLS("ALTERTABLE_REPLACECOLS", HiveParser.TOK_ALTERTABLE_REPLACECOLS,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_RENAMECOL("ALTERTABLE_RENAMECOL", HiveParser.TOK_ALTERTABLE_RENAMECOL,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_RENAMEPART("ALTERTABLE_RENAMEPART", HiveParser.TOK_ALTERTABLE_RENAMEPART, new Privilege[]{Privilege.DROP},
      new Privilege[]{Privilege.CREATE}),
  ALTERTABLE_UPDATEPARTSTATS("ALTERTABLE_UPDATEPARTSTATS",
      new int[] {HiveParser.TOK_ALTERPARTITION_UPDATESTATS, HiveParser.TOK_ALTERPARTITION_UPDATECOLSTATS},
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_UPDATETABLESTATS("ALTERTABLE_UPDATETABLESTATS",
      new int[] {HiveParser.TOK_ALTERTABLE_UPDATESTATS, HiveParser.TOK_ALTERTABLE_UPDATECOLSTATS},
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_RENAME("ALTERTABLE_RENAME", HiveParser.TOK_ALTERTABLE_RENAME, new Privilege[]{Privilege.ALTER_METADATA},
      null),
  ALTERTABLE_DROPPARTS("ALTERTABLE_DROPPARTS",
      new int[] {HiveParser.TOK_ALTERTABLE_DROPPARTS, HiveParser.TOK_ALTERVIEW_DROPPARTS},
      new Privilege[]{Privilege.DROP}, null),
  // The location is input and table is output for alter-table add partitions
  ALTERTABLE_ADDPARTS("ALTERTABLE_ADDPARTS",
      new int[] {HiveParser.TOK_ALTERTABLE_ADDPARTS, HiveParser.TOK_ALTERVIEW_ADDPARTS}, null,
      new Privilege[]{Privilege.CREATE}),
  ALTERTABLE_TOUCH("ALTERTABLE_TOUCH", HiveParser.TOK_ALTERTABLE_TOUCH, null, null),
  ALTERTABLE_ARCHIVE("ALTERTABLE_ARCHIVE", HiveParser.TOK_ALTERTABLE_ARCHIVE, new Privilege[]{Privilege.ALTER_DATA},
      null),
  ALTERTABLE_UNARCHIVE("ALTERTABLE_UNARCHIVE", HiveParser.TOK_ALTERTABLE_UNARCHIVE,
      new Privilege[]{Privilege.ALTER_DATA}, null),
  ALTERTABLE_PROPERTIES("ALTERTABLE_PROPERTIES",
      new int[] {HiveParser.TOK_ALTERTABLE_PROPERTIES, HiveParser.TOK_ALTERTABLE_DROPPROPERTIES},
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_OWNER("ALTERTABLE_OWNER", HiveParser.TOK_ALTERTABLE_OWNER, null, null),
  ALTERTABLE_SETPARTSPEC("ALTERTABLE_SETPARTSPEC", HiveParser.TOK_ALTERTABLE_SETPARTSPEC, null, null),
  ALTERTABLE_EXECUTE("ALTERTABLE_EXECUTE", HiveParser.TOK_ALTERTABLE_EXECUTE, null, null),
  ALTERTABLE_CREATEBRANCH("ALTERTABLE_CREATEBRANCH", HiveParser.TOK_ALTERTABLE_CREATE_BRANCH, null, null),
  ALTERTABLE_CREATETAG("ALTERTABLE_CREATETAG", HiveParser.TOK_ALTERTABLE_CREATE_TAG, null, null),
  ALTERTABLE_DROPBRANCH("ALTERTABLE_DROPBRANCH", HiveParser.TOK_ALTERTABLE_DROP_BRANCH, null, null),
  ALTERTABLE_RENAMEBRANCH("ALTERTABLE_RENAMEBRANCH", HiveParser.TOK_ALTERTABLE_RENAME_BRANCH, null, null),
  ALTERTABLE_REPLACESNAPSHOTREF("ALTERTABLE_REPLACESNAPSHOTREF", HiveParser.TOK_ALTERTABLE_REPLACE_SNAPSHOTREF, null, null),
  ALTERTABLE_DROPTAG("ALTERTABLE_DROPTAG", HiveParser.TOK_ALTERTABLE_DROP_TAG, null, null),
  ALTERTABLE_CONVERT("ALTERTABLE_CONVERT", HiveParser.TOK_ALTERTABLE_CONVERT, null, null),
  ALTERTABLE_SERIALIZER("ALTERTABLE_SERIALIZER", HiveParser.TOK_ALTERTABLE_SERIALIZER,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERPARTITION_SERIALIZER("ALTERPARTITION_SERIALIZER", HiveParser.TOK_ALTERPARTITION_SERIALIZER,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_SERDEPROPERTIES("ALTERTABLE_SERDEPROPERTIES",
      new int[] {HiveParser.TOK_ALTERTABLE_SETSERDEPROPERTIES, HiveParser.TOK_ALTERTABLE_UNSETSERDEPROPERTIES},
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERPARTITION_SERDEPROPERTIES("ALTERPARTITION_SERDEPROPERTIES",
      new int[] {HiveParser.TOK_ALTERPARTITION_SETSERDEPROPERTIES, HiveParser.TOK_ALTERPARTITION_UNSETSERDEPROPERTIES},
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_CLUSTER_SORT("ALTERTABLE_CLUSTER_SORT", HiveParser.TOK_ALTERTABLE_CLUSTER_SORT,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ANALYZE_TABLE("ANALYZE_TABLE", HiveParser.TOK_ANALYZE, null, null),
  CACHE_METADATA("CACHE_METADATA", HiveParser.TOK_CACHE_METADATA, new Privilege[]{Privilege.SELECT}, null),
  ALTERTABLE_BUCKETNUM("ALTERTABLE_BUCKETNUM", HiveParser.TOK_ALTERTABLE_BUCKETS,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERPARTITION_BUCKETNUM("ALTERPARTITION_BUCKETNUM", HiveParser.TOK_ALTERPARTITION_BUCKETS,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  SHOWDATABASES("SHOWDATABASES", HiveParser.TOK_SHOWDATABASES, new Privilege[]{Privilege.SHOW_DATABASE}, null, true,
      false),
  SHOWDATACONNECTORS("SHOWDATACONNECTORS", HiveParser.TOK_SHOWDATACONNECTORS, new Privilege[]{Privilege.SHOW_DATABASE}, null, true,
      false),
  SHOWTABLES("SHOWTABLES", HiveParser.TOK_SHOWTABLES, null, null, true, false),
  SHOWCOLUMNS("SHOWCOLUMNS", HiveParser.TOK_SHOWCOLUMNS, null, null, true, false),
  SHOW_TABLESTATUS("SHOW_TABLESTATUS", HiveParser.TOK_SHOW_TABLESTATUS, null, null, true, false),
  SHOW_TBLPROPERTIES("SHOW_TBLPROPERTIES", HiveParser.TOK_SHOW_TBLPROPERTIES, null, null, true, false),
  SHOW_CREATEDATABASE("SHOW_CREATEDATABASE", HiveParser.TOK_SHOW_CREATEDATABASE, new Privilege[]{Privilege.SELECT},
      null),
  SHOW_CREATETABLE("SHOW_CREATETABLE", HiveParser.TOK_SHOW_CREATETABLE, new Privilege[]{Privilege.SELECT}, null),
  SHOWFUNCTIONS("SHOWFUNCTIONS", HiveParser.TOK_SHOWFUNCTIONS, null, null, true, false),
  SHOWPARTITIONS("SHOWPARTITIONS", HiveParser.TOK_SHOWPARTITIONS, null, null),
  SHOWLOCKS("SHOWLOCKS", new int[] {HiveParser.TOK_SHOWLOCKS, HiveParser.TOK_SHOWDBLOCKS}, null, null, true, false),
  SHOWCONF("SHOWCONF", HiveParser.TOK_SHOWCONF, null, null),
  SHOWVIEWS("SHOWVIEWS", HiveParser.TOK_SHOWVIEWS, null, null, true, false),
  SHOWMATERIALIZEDVIEWS("SHOWMATERIALIZEDVIEWS", HiveParser.TOK_SHOWMATERIALIZEDVIEWS, null, null, true, false),
  CREATEFUNCTION("CREATEFUNCTION", HiveParser.TOK_CREATEFUNCTION, null, null),
  DROPFUNCTION("DROPFUNCTION", HiveParser.TOK_DROPFUNCTION, null, null),
  RELOADFUNCTION("RELOADFUNCTION", HiveParser.TOK_RELOADFUNCTIONS, null, null),
  CREATEMACRO("CREATEMACRO", HiveParser.TOK_CREATEMACRO, null, null),
  DROPMACRO("DROPMACRO", HiveParser.TOK_DROPMACRO, null, null),
  CREATEVIEW("CREATEVIEW", new int[] {HiveParser.TOK_CREATEVIEW, HiveParser.TOK_ALTERVIEW_AS},
      new Privilege[]{Privilege.SELECT}, new Privilege[]{Privilege.CREATE}),
  CREATE_MATERIALIZED_VIEW("CREATE_MATERIALIZED_VIEW", HiveParser.TOK_CREATE_MATERIALIZED_VIEW,
      new Privilege[]{Privilege.SELECT}, new Privilege[]{Privilege.CREATE}),
  DROPVIEW("DROPVIEW", HiveParser.TOK_DROPVIEW, null, new Privilege[]{Privilege.DROP}),
  DROP_MATERIALIZED_VIEW("DROP_MATERIALIZED_VIEW", HiveParser.TOK_DROP_MATERIALIZED_VIEW, null,
      new Privilege[]{Privilege.DROP}),
  ALTER_MATERIALIZED_VIEW_REWRITE("ALTER_MATERIALIZED_VIEW_REWRITE", HiveParser.TOK_ALTER_MATERIALIZED_VIEW_REWRITE,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTER_MATERIALIZED_VIEW_REBUILD("ALTER_MATERIALIZED_VIEW_REBUILD", HiveParser.TOK_ALTER_MATERIALIZED_VIEW_REBUILD,
          new Privilege[]{Privilege.SELECT}, new Privilege[]{Privilege.CREATE, Privilege.DROP}),
  ALTERVIEW_PROPERTIES("ALTERVIEW_PROPERTIES",
      new int[] {HiveParser.TOK_ALTERVIEW_PROPERTIES, HiveParser.TOK_ALTERVIEW_DROPPROPERTIES}, null, null),
  LOCKTABLE("LOCKTABLE", HiveParser.TOK_LOCKTABLE, new Privilege[]{Privilege.LOCK}, null),
  UNLOCKTABLE("UNLOCKTABLE", HiveParser.TOK_UNLOCKTABLE, new Privilege[]{Privilege.LOCK}, null),
  CREATEROLE("CREATEROLE", HiveParser.TOK_CREATEROLE, null, null),
  DROPROLE("DROPROLE", HiveParser.TOK_DROPROLE, null, null),
  GRANT_PRIVILEGE("GRANT_PRIVILEGE", HiveParser.TOK_GRANT, null, null),
  REVOKE_PRIVILEGE("REVOKE_PRIVILEGE", HiveParser.TOK_REVOKE, null, null),
  SHOW_GRANT("SHOW_GRANT", HiveParser.TOK_SHOW_GRANT, null, null, true, false),
  GRANT_ROLE("GRANT_ROLE", HiveParser.TOK_GRANT_ROLE, null, null),
  REVOKE_ROLE("REVOKE_ROLE", HiveParser.TOK_REVOKE_ROLE, null, null),
  SHOW_ROLES("SHOW_ROLES",
      new int[] {HiveParser.TOK_SHOW_ROLES, HiveParser.TOK_SHOW_CURRENT_ROLE, HiveParser.TOK_SET_ROLE},
      null, null, true, false),
  SHOW_ROLE_PRINCIPALS("SHOW_ROLE_PRINCIPALS", HiveParser.TOK_SHOW_ROLE_PRINCIPALS, null, null, true, false),
  SHOW_ROLE_GRANT("SHOW_ROLE_GRANT", HiveParser.TOK_SHOW_ROLE_GRANT, null, null, true, false),
  ALTERTABLE_FILEFORMAT("ALTERTABLE_FILEFORMAT", HiveParser.TOK_ALTERTABLE_FILEFORMAT,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERPARTITION_FILEFORMAT("ALTERPARTITION_FILEFORMAT", HiveParser.TOK_ALTERPARTITION_FILEFORMAT,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_LOCATION("ALTERTABLE_LOCATION", HiveParser.TOK_ALTERTABLE_LOCATION, new Privilege[]{Privilege.ALTER_DATA},
      null),
  ALTERPARTITION_LOCATION("ALTERPARTITION_LOCATION", HiveParser.TOK_ALTERPARTITION_LOCATION,
      new Privilege[]{Privilege.ALTER_DATA}, null),
  CREATETABLE("CREATETABLE", HiveParser.TOK_CREATETABLE, null, new Privilege[]{Privilege.CREATE}),
  TRUNCATETABLE("TRUNCATETABLE", HiveParser.TOK_TRUNCATETABLE, null, new Privilege[]{Privilege.DROP}),
  CREATETABLE_AS_SELECT("CREATETABLE_AS_SELECT", (int[])null, new Privilege[]{Privilege.SELECT},
      new Privilege[]{Privilege.CREATE}),
  QUERY("QUERY", HiveParser.TOK_QUERY, new Privilege[]{Privilege.SELECT},
      new Privilege[]{Privilege.ALTER_DATA, Privilege.CREATE}, true, false),
  ALTERDATABASE("ALTERDATABASE", HiveParser.TOK_ALTERDATABASE_PROPERTIES, null, null),
  ALTERDATABASE_OWNER("ALTERDATABASE_OWNER", HiveParser.TOK_ALTERDATABASE_OWNER, null, null),
  ALTERDATABASE_LOCATION("ALTERDATABASE_LOCATION",
      new int[] {HiveParser.TOK_ALTERDATABASE_LOCATION, HiveParser.TOK_ALTERDATABASE_MANAGEDLOCATION},
      new Privilege[]{Privilege.ALTER_DATA}, null),
  DESCDATABASE("DESCDATABASE", HiveParser.TOK_DESCDATABASE, null, null),
  ALTERDATACONNECTOR("ALTERDATACONNECTOR", HiveParser.TOK_ALTERDATACONNECTOR_PROPERTIES, null, null),
  ALTERDATACONNECTOR_OWNER("ALTERDATABASE_OWNER", HiveParser.TOK_ALTERDATACONNECTOR_OWNER, null, null),
  ALTERDATACONNECTOR_URL("ALTERDATACONNECTOR_", HiveParser.TOK_ALTERDATACONNECTOR_URL, null, null),
  DESCDATACONNECTOR("DESCDATACONNECTOR", HiveParser.TOK_DESCDATACONNECTOR, null, null),
  ALTERTABLE_MERGEFILES("ALTER_TABLE_MERGE", HiveParser.TOK_ALTERTABLE_MERGEFILES, new Privilege[] {Privilege.SELECT},
      new Privilege[] {Privilege.ALTER_DATA}),
  ALTERPARTITION_MERGEFILES("ALTER_PARTITION_MERGE", HiveParser.TOK_ALTERPARTITION_MERGEFILES,
      new Privilege[] {Privilege.SELECT}, new Privilege[] {Privilege.ALTER_DATA}),
  ALTERTABLE_SKEWED("ALTERTABLE_SKEWED", HiveParser.TOK_ALTERTABLE_SKEWED, new Privilege[] {Privilege.ALTER_METADATA},
      null),
  ALTERTBLPART_SKEWED_LOCATION("ALTERTBLPART_SKEWED_LOCATION", HiveParser.TOK_ALTERTABLE_SKEWED_LOCATION,
      new Privilege[] {Privilege.ALTER_DATA}, null),
  ALTERTABLE_PARTCOLTYPE("ALTERTABLE_PARTCOLTYPE", HiveParser.TOK_ALTERTABLE_PARTCOLTYPE,
      new Privilege[] {Privilege.SELECT}, new Privilege[] {Privilege.ALTER_DATA}),
  ALTERTABLE_EXCHANGEPARTITION("ALTERTABLE_EXCHANGEPARTITION", HiveParser.TOK_ALTERTABLE_EXCHANGEPARTITION,
      new Privilege[] {Privilege.SELECT, Privilege.DELETE}, new Privilege[] {Privilege.INSERT}),
  ALTERTABLE_DROPCONSTRAINT("ALTERTABLE_DROPCONSTRAINT", HiveParser.TOK_ALTERTABLE_DROPCONSTRAINT,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_ADDCONSTRAINT("ALTERTABLE_ADDCONSTRAINT", HiveParser.TOK_ALTERTABLE_ADDCONSTRAINT,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_UPDATECOLUMNS("ALTERTABLE_UPDATECOLUMNS", HiveParser.TOK_ALTERTABLE_UPDATECOLUMNS,
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERVIEW_RENAME("ALTERVIEW_RENAME", HiveParser.TOK_ALTERVIEW_RENAME, new Privilege[] {Privilege.ALTER_METADATA},
      null),
  ALTERVIEW_AS("ALTERVIEW_AS", HiveParser.TOK_ALTERVIEW, new Privilege[] {Privilege.ALTER_METADATA}, null),
  ALTERTABLE_COMPACT("ALTERTABLE_COMPACT", HiveParser.TOK_ALTERTABLE_COMPACT, new Privilege[]{Privilege.SELECT},
      new Privilege[]{Privilege.ALTER_DATA}),
  SHOW_COMPACTIONS("SHOW COMPACTIONS", HiveParser.TOK_SHOW_COMPACTIONS, null, null, true, false),
  SHOW_TRANSACTIONS("SHOW TRANSACTIONS", HiveParser.TOK_SHOW_TRANSACTIONS, null, null, true, false),
  START_TRANSACTION("START TRANSACTION", HiveParser.TOK_START_TRANSACTION, null, null, false, false),
  COMMIT("COMMIT", HiveParser.TOK_COMMIT, null, null, true, true),
  ROLLBACK("ROLLBACK", HiveParser.TOK_ROLLBACK, null, null, true, true),
  SET_AUTOCOMMIT("SET AUTOCOMMIT", HiveParser.TOK_SET_AUTOCOMMIT, null, null, true, false),
  ABORT_TRANSACTIONS("ABORT TRANSACTIONS", HiveParser.TOK_ABORT_TRANSACTIONS, null, null, false, false),
  ABORT_COMPACTION("ABORT COMPACTIONS", HiveParser.TOK_ABORT_COMPACTIONS, null, null, false, false),

  KILL_QUERY("KILL QUERY", HiveParser.TOK_KILL_QUERY, null, null),
  CREATE_RESOURCEPLAN("CREATE RESOURCEPLAN", HiveParser.TOK_CREATE_RP, null, null, false, false),
  SHOW_RESOURCEPLAN("SHOW RESOURCEPLAN", HiveParser.TOK_SHOW_RP, null, null, false, false),
  ALTER_RESOURCEPLAN("ALTER RESOURCEPLAN", new int[] {HiveParser.TOK_ALTER_RP_VALIDATE, HiveParser.TOK_ALTER_RP_RENAME,
      HiveParser.TOK_ALTER_RP_SET, HiveParser.TOK_ALTER_RP_UNSET, HiveParser.TOK_ALTER_RP_ENABLE,
      HiveParser.TOK_ALTER_RP_DISABLE, HiveParser.TOK_ALTER_RP_REPLACE}, null, null, false, false),
  DROP_RESOURCEPLAN("DROP RESOURCEPLAN", HiveParser.TOK_DROP_RP, null, null, false, false),
  CREATE_TRIGGER("CREATE TRIGGER", HiveParser.TOK_CREATE_TRIGGER, null, null, false, false),
  ALTER_TRIGGER("ALTER TRIGGER", HiveParser.TOK_ALTER_TRIGGER, null, null, false, false),
  DROP_TRIGGER("DROP TRIGGER", HiveParser.TOK_DROP_TRIGGER, null, null, false, false),
  CREATE_POOL("CREATE POOL", HiveParser.TOK_CREATE_POOL, null, null, false, false),
  ALTER_POOL("ALTER POOL", new int[] {HiveParser.TOK_ALTER_POOL, HiveParser.TOK_ALTER_POOL_ADD_TRIGGER,
      HiveParser.TOK_ALTER_POOL_DROP_TRIGGER}, null, null, false, false),
  DROP_POOL("DROP POOL", HiveParser.TOK_DROP_POOL, null, null, false, false),
  CREATE_MAPPING("CREATE MAPPING", HiveParser.TOK_CREATE_MAPPING, null, null, false, false),
  ALTER_MAPPING("ALTER MAPPING", HiveParser.TOK_ALTER_MAPPING, null, null, false, false),
  DROP_MAPPING("DROP MAPPING", HiveParser.TOK_DROP_MAPPING, null, null, false, false),
  CREATE_SCHEDULED_QUERY("CREATE SCHEDULED QUERY", HiveParser.TOK_CREATE_SCHEDULED_QUERY, null, null),
  ALTER_SCHEDULED_QUERY("ALTER SCHEDULED QUERY", HiveParser.TOK_ALTER_SCHEDULED_QUERY, null, null),
  DROP_SCHEDULED_QUERY("DROP SCHEDULED QUERY", HiveParser.TOK_DROP_SCHEDULED_QUERY, null, null),
  PREPARE("PREPARE QUERY", HiveParser.TOK_PREPARE, null, null),
  EXECUTE("EXECUTE QUERY", HiveParser.TOK_EXECUTE, null, null)
  ;

  private final String operationName;
  private final int[] tokens;
  private final Privilege[] inputRequiredPrivileges;
  private final Privilege[] outputRequiredPrivileges;

  /**
   * Only a small set of operations is allowed inside an explicit transactions, e.g. DML on
   * Acid tables or ops w/o persistent side effects like USE DATABASE, SHOW TABLES, etc so
   * that rollback is meaningful
   * todo: mark all operations appropriately
   */
  private final boolean allowedInTransaction;
  private final boolean requiresOpenTransaction;

  HiveOperation(String operationName, int token, Privilege[] inputRequiredPrivileges,
      Privilege[] outputRequiredPrivileges) {
    this(operationName, new int[] {token}, inputRequiredPrivileges, outputRequiredPrivileges, false, false);
  }

  HiveOperation(String operationName, int[] tokens, Privilege[] inputRequiredPrivileges,
      Privilege[] outputRequiredPrivileges) {
    this(operationName, tokens, inputRequiredPrivileges, outputRequiredPrivileges, false, false);
  }

  HiveOperation(String operationName, int token, Privilege[] inputRequiredPrivileges,
      Privilege[] outputRequiredPrivileges, boolean allowedInTransaction, boolean requiresOpenTransaction) {
    this(operationName, new int[] {token}, inputRequiredPrivileges, outputRequiredPrivileges, allowedInTransaction,
        requiresOpenTransaction);
  }

  HiveOperation(String operationName, int[] tokens, Privilege[] inputRequiredPrivileges,
      Privilege[] outputRequiredPrivileges, boolean allowedInTransaction, boolean requiresOpenTransaction) {
    this.operationName = operationName;
    this.tokens = tokens;
    this.inputRequiredPrivileges = inputRequiredPrivileges;
    this.outputRequiredPrivileges = outputRequiredPrivileges;
    this.requiresOpenTransaction = requiresOpenTransaction;
    this.allowedInTransaction = allowedInTransaction || requiresOpenTransaction;
  }

  public String getOperationName() {
    return operationName;
  }

  public Privilege[] getInputRequiredPrivileges() {
    return inputRequiredPrivileges;
  }

  public Privilege[] getOutputRequiredPrivileges() {
    return outputRequiredPrivileges;
  }

  public boolean isAllowedInTransaction() {
    return allowedInTransaction;
  }

  public boolean isRequiresOpenTransaction() {
    return requiresOpenTransaction;
  }

  private static final Map<Integer, HiveOperation> TOKEN_TO_OPERATION = new HashMap<>();
  static {
    for (HiveOperation hiveOperation : values()) {
      if (hiveOperation.tokens != null) {
        for (int token : hiveOperation.tokens) {
          TOKEN_TO_OPERATION.put(token, hiveOperation);
        }
      }
    }
  }

  public static HiveOperation operationForToken(int token) {
    return TOKEN_TO_OPERATION.get(token);
  }
}
