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

import org.apache.hadoop.hive.ql.security.authorization.Privilege;

public enum HiveOperation {
  EXPLAIN("EXPLAIN", null, null),
  LOAD("LOAD", null, new Privilege[]{Privilege.ALTER_DATA}),
  EXPORT("EXPORT", new Privilege[]{Privilege.SELECT}, null),
  IMPORT("IMPORT", null, new Privilege[]{Privilege.ALTER_METADATA, Privilege.ALTER_DATA}),
  REPLDUMP("REPLDUMP", new Privilege[]{Privilege.ALL}, null),
  REPLLOAD("REPLLOAD", null, new Privilege[]{Privilege.ALL}),
  REPLSTATUS("REPLSTATUS", new Privilege[]{Privilege.SELECT}, null),
  CREATEDATABASE("CREATEDATABASE", null, new Privilege[]{Privilege.CREATE}),
  DROPDATABASE("DROPDATABASE", null, new Privilege[]{Privilege.DROP}),
  SWITCHDATABASE("SWITCHDATABASE", null, null, true, false),
  LOCKDB("LOCKDATABASE",  new Privilege[]{Privilege.LOCK}, null),
  UNLOCKDB("UNLOCKDATABASE",  new Privilege[]{Privilege.LOCK}, null),
  DROPTABLE ("DROPTABLE", null, new Privilege[]{Privilege.DROP}),
  DESCTABLE("DESCTABLE", null, null),
  DESCFUNCTION("DESCFUNCTION", null, null),
  MSCK("MSCK", null, null),
  ALTERTABLE_ADDCOLS("ALTERTABLE_ADDCOLS", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_REPLACECOLS("ALTERTABLE_REPLACECOLS", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_RENAMECOL("ALTERTABLE_RENAMECOL", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_RENAMEPART("ALTERTABLE_RENAMEPART", new Privilege[]{Privilege.DROP}, new Privilege[]{Privilege.CREATE}),
  ALTERTABLE_UPDATEPARTSTATS("ALTERTABLE_UPDATEPARTSTATS", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_UPDATETABLESTATS("ALTERTABLE_UPDATETABLESTATS", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_RENAME("ALTERTABLE_RENAME", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_DROPPARTS("ALTERTABLE_DROPPARTS", new Privilege[]{Privilege.DROP}, null),
  // The location is input and table is output for alter-table add partitions
  ALTERTABLE_ADDPARTS("ALTERTABLE_ADDPARTS", null, new Privilege[]{Privilege.CREATE}),
  ALTERTABLE_TOUCH("ALTERTABLE_TOUCH", null, null),
  ALTERTABLE_ARCHIVE("ALTERTABLE_ARCHIVE", new Privilege[]{Privilege.ALTER_DATA}, null),
  ALTERTABLE_UNARCHIVE("ALTERTABLE_UNARCHIVE", new Privilege[]{Privilege.ALTER_DATA}, null),
  ALTERTABLE_PROPERTIES("ALTERTABLE_PROPERTIES", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_SERIALIZER("ALTERTABLE_SERIALIZER", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERPARTITION_SERIALIZER("ALTERPARTITION_SERIALIZER", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_SERDEPROPERTIES("ALTERTABLE_SERDEPROPERTIES", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERPARTITION_SERDEPROPERTIES("ALTERPARTITION_SERDEPROPERTIES", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_CLUSTER_SORT("ALTERTABLE_CLUSTER_SORT",
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ANALYZE_TABLE("ANALYZE_TABLE", null, null),
  CACHE_METADATA("CACHE_METADATA", new Privilege[]{Privilege.SELECT}, null),
  ALTERTABLE_BUCKETNUM("ALTERTABLE_BUCKETNUM",
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERPARTITION_BUCKETNUM("ALTERPARTITION_BUCKETNUM",
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  SHOWDATABASES("SHOWDATABASES", new Privilege[]{Privilege.SHOW_DATABASE}, null, true, false),
  SHOWTABLES("SHOWTABLES", null, null, true, false),
  SHOWCOLUMNS("SHOWCOLUMNS", null, null, true, false),
  SHOW_TABLESTATUS("SHOW_TABLESTATUS", null, null, true, false),
  SHOW_TBLPROPERTIES("SHOW_TBLPROPERTIES", null, null, true, false),
  SHOW_CREATEDATABASE("SHOW_CREATEDATABASE", new Privilege[]{Privilege.SELECT}, null),
  SHOW_CREATETABLE("SHOW_CREATETABLE", new Privilege[]{Privilege.SELECT}, null),
  SHOWFUNCTIONS("SHOWFUNCTIONS", null, null, true, false),
  SHOWPARTITIONS("SHOWPARTITIONS", null, null),
  SHOWLOCKS("SHOWLOCKS", null, null, true, false),
  SHOWCONF("SHOWCONF", null, null),
  SHOWVIEWS("SHOWVIEWS", null, null, true, false),
  SHOWMATERIALIZEDVIEWS("SHOWMATERIALIZEDVIEWS", null, null, true, false),
  CREATEFUNCTION("CREATEFUNCTION", null, null),
  DROPFUNCTION("DROPFUNCTION", null, null),
  RELOADFUNCTION("RELOADFUNCTION", null, null),
  CREATEMACRO("CREATEMACRO", null, null),
  DROPMACRO("DROPMACRO", null, null),
  CREATEVIEW("CREATEVIEW", new Privilege[]{Privilege.SELECT}, new Privilege[]{Privilege.CREATE}),
  CREATE_MATERIALIZED_VIEW("CREATE_MATERIALIZED_VIEW", new Privilege[]{Privilege.SELECT}, new
      Privilege[]{Privilege.CREATE}),
  DROPVIEW("DROPVIEW", null, new Privilege[]{Privilege.DROP}),
  DROP_MATERIALIZED_VIEW("DROP_MATERIALIZED_VIEW", null, new Privilege[]{Privilege.DROP}),
  ALTER_MATERIALIZED_VIEW_REWRITE("ALTER_MATERIALIZED_VIEW_REWRITE",
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERVIEW_PROPERTIES("ALTERVIEW_PROPERTIES", null, null),
  DROPVIEW_PROPERTIES("DROPVIEW_PROPERTIES", null, null),
  LOCKTABLE("LOCKTABLE",  new Privilege[]{Privilege.LOCK}, null),
  UNLOCKTABLE("UNLOCKTABLE",  new Privilege[]{Privilege.LOCK}, null),
  CREATEROLE("CREATEROLE", null, null),
  DROPROLE("DROPROLE", null, null),
  GRANT_PRIVILEGE("GRANT_PRIVILEGE", null, null),
  REVOKE_PRIVILEGE("REVOKE_PRIVILEGE", null, null),
  SHOW_GRANT("SHOW_GRANT", null, null, true, false),
  GRANT_ROLE("GRANT_ROLE", null, null),
  REVOKE_ROLE("REVOKE_ROLE", null, null),
  SHOW_ROLES("SHOW_ROLES", null, null, true, false),
  SHOW_ROLE_PRINCIPALS("SHOW_ROLE_PRINCIPALS", null, null, true, false),
  SHOW_ROLE_GRANT("SHOW_ROLE_GRANT", null, null, true, false),
  ALTERTABLE_FILEFORMAT("ALTERTABLE_FILEFORMAT", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERPARTITION_FILEFORMAT("ALTERPARTITION_FILEFORMAT", new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_LOCATION("ALTERTABLE_LOCATION", new Privilege[]{Privilege.ALTER_DATA}, null),
  ALTERPARTITION_LOCATION("ALTERPARTITION_LOCATION", new Privilege[]{Privilege.ALTER_DATA}, null),
  CREATETABLE("CREATETABLE", null, new Privilege[]{Privilege.CREATE}),
  TRUNCATETABLE("TRUNCATETABLE", null, new Privilege[]{Privilege.DROP}),
  CREATETABLE_AS_SELECT("CREATETABLE_AS_SELECT", new Privilege[]{Privilege.SELECT}, new Privilege[]{Privilege.CREATE}),
  QUERY("QUERY", new Privilege[]{Privilege.SELECT}, new Privilege[]{Privilege.ALTER_DATA, Privilege.CREATE}, true, false),
  ALTERDATABASE("ALTERDATABASE", null, null),
  ALTERDATABASE_OWNER("ALTERDATABASE_OWNER", null, null),
  ALTERDATABASE_LOCATION("ALTERDATABASE_LOCATION", new Privilege[]{Privilege.ALTER_DATA}, null),
  DESCDATABASE("DESCDATABASE", null, null),
  ALTERTABLE_MERGEFILES("ALTER_TABLE_MERGE", new Privilege[] { Privilege.SELECT }, new Privilege[] { Privilege.ALTER_DATA }),
  ALTERPARTITION_MERGEFILES("ALTER_PARTITION_MERGE", new Privilege[] { Privilege.SELECT }, new Privilege[] { Privilege.ALTER_DATA }),
  ALTERTABLE_SKEWED("ALTERTABLE_SKEWED", new Privilege[] {Privilege.ALTER_METADATA}, null),
  ALTERTBLPART_SKEWED_LOCATION("ALTERTBLPART_SKEWED_LOCATION",
      new Privilege[] {Privilege.ALTER_DATA}, null),
  ALTERTABLE_PARTCOLTYPE("ALTERTABLE_PARTCOLTYPE", new Privilege[] { Privilege.SELECT }, new Privilege[] { Privilege.ALTER_DATA }),
  ALTERTABLE_EXCHANGEPARTITION(
      "ALTERTABLE_EXCHANGEPARTITION", new Privilege[] { Privilege.SELECT, Privilege.DELETE },
      new Privilege[] { Privilege.INSERT }),
  ALTERTABLE_DROPCONSTRAINT("ALTERTABLE_DROPCONSTRAINT",
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERTABLE_ADDCONSTRAINT("ALTERTABLE_ADDCONSTRAINT",
      new Privilege[]{Privilege.ALTER_METADATA}, null),
  ALTERVIEW_RENAME("ALTERVIEW_RENAME", new Privilege[] {Privilege.ALTER_METADATA}, null),
  ALTERVIEW_AS("ALTERVIEW_AS", new Privilege[] {Privilege.ALTER_METADATA}, null),
  ALTERTABLE_COMPACT("ALTERTABLE_COMPACT", new Privilege[]{Privilege.SELECT}, new Privilege[]{Privilege.ALTER_DATA}),
  SHOW_COMPACTIONS("SHOW COMPACTIONS", null, null, true, false),
  SHOW_TRANSACTIONS("SHOW TRANSACTIONS", null, null, true, false),
  START_TRANSACTION("START TRANSACTION", null, null, false, false),
  COMMIT("COMMIT", null, null, true, true),
  ROLLBACK("ROLLBACK", null, null, true, true),
  SET_AUTOCOMMIT("SET AUTOCOMMIT", null, null, true, false),
  ABORT_TRANSACTIONS("ABORT TRANSACTIONS", null, null, false, false),
  KILL_QUERY("KILL QUERY", null, null),
  CREATE_RESOURCEPLAN("CREATE RESOURCEPLAN", null, null, false, false),
  SHOW_RESOURCEPLAN("SHOW RESOURCEPLAN", null, null, false, false),
  ALTER_RESOURCEPLAN("ALTER RESOURCEPLAN", null, null, false, false),
  DROP_RESOURCEPLAN("DROP RESOURCEPLAN", null, null, false, false),
  CREATE_TRIGGER("CREATE TRIGGER", null, null, false, false),
  ALTER_TRIGGER("ALTER TRIGGER", null, null, false, false),
  DROP_TRIGGER("DROP TRIGGER", null, null, false, false),
  CREATE_POOL("CREATE POOL", null, null, false, false),
  ALTER_POOL("ALTER POOL", null, null, false, false),
  DROP_POOL("DROP POOL", null, null, false, false),
  CREATE_MAPPING("CREATE MAPPING", null, null, false, false),
  ALTER_MAPPING("ALTER MAPPING", null, null, false, false),
  DROP_MAPPING("DROP MAPPING", null, null, false, false);


  private String operationName;

  private Privilege[] inputRequiredPrivileges;

  private Privilege[] outputRequiredPrivileges;

  /**
   * Only a small set of operations is allowed inside an explicit transactions, e.g. DML on
   * Acid tables or ops w/o persistent side effects like USE DATABASE, SHOW TABLES, etc so
   * that rollback is meaningful
   * todo: mark all operations appropriately
   */
  private final boolean allowedInTransaction;
  private final boolean requiresOpenTransaction;

  public Privilege[] getInputRequiredPrivileges() {
    return inputRequiredPrivileges;
  }

  public Privilege[] getOutputRequiredPrivileges() {
    return outputRequiredPrivileges;
  }

  public String getOperationName() {
    return operationName;
  }

  public boolean isAllowedInTransaction() {
    return allowedInTransaction;
  }
  public boolean isRequiresOpenTransaction() { return requiresOpenTransaction; }

  private HiveOperation(String operationName,
                        Privilege[] inputRequiredPrivileges, Privilege[] outputRequiredPrivileges) {
    this(operationName, inputRequiredPrivileges, outputRequiredPrivileges, false, false);
  }
  private HiveOperation(String operationName,
      Privilege[] inputRequiredPrivileges, Privilege[] outputRequiredPrivileges,
      boolean allowedInTransaction, boolean requiresOpenTransaction) {
    this.operationName = operationName;
    this.inputRequiredPrivileges = inputRequiredPrivileges;
    this.outputRequiredPrivileges = outputRequiredPrivileges;
    this.requiresOpenTransaction = requiresOpenTransaction;
    if(requiresOpenTransaction) {
      allowedInTransaction = true;
    }
    this.allowedInTransaction = allowedInTransaction;
  }

  public static class PrivilegeAgreement {

    private Privilege[] inputUserLevelRequiredPriv;
    private Privilege[] inputDBLevelRequiredPriv;
    private Privilege[] inputTableLevelRequiredPriv;
    private Privilege[] inputColumnLevelRequiredPriv;
    private Privilege[] outputUserLevelRequiredPriv;
    private Privilege[] outputDBLevelRequiredPriv;
    private Privilege[] outputTableLevelRequiredPriv;
    private Privilege[] outputColumnLevelRequiredPriv;

    public PrivilegeAgreement putUserLevelRequiredPriv(
        Privilege[] inputUserLevelRequiredPriv,
        Privilege[] outputUserLevelRequiredPriv) {
      this.inputUserLevelRequiredPriv = inputUserLevelRequiredPriv;
      this.outputUserLevelRequiredPriv = outputUserLevelRequiredPriv;
      return this;
    }

    public PrivilegeAgreement putDBLevelRequiredPriv(
        Privilege[] inputDBLevelRequiredPriv,
        Privilege[] outputDBLevelRequiredPriv) {
      this.inputDBLevelRequiredPriv = inputDBLevelRequiredPriv;
      this.outputDBLevelRequiredPriv = outputDBLevelRequiredPriv;
      return this;
    }

    public PrivilegeAgreement putTableLevelRequiredPriv(
        Privilege[] inputTableLevelRequiredPriv,
        Privilege[] outputTableLevelRequiredPriv) {
      this.inputTableLevelRequiredPriv = inputTableLevelRequiredPriv;
      this.outputTableLevelRequiredPriv = outputTableLevelRequiredPriv;
      return this;
    }

    public PrivilegeAgreement putColumnLevelRequiredPriv(
        Privilege[] inputColumnLevelPriv, Privilege[] outputColumnLevelPriv) {
      this.inputColumnLevelRequiredPriv = inputColumnLevelPriv;
      this.outputColumnLevelRequiredPriv = outputColumnLevelPriv;
      return this;
    }

    public Privilege[] getInputUserLevelRequiredPriv() {
      return inputUserLevelRequiredPriv;
    }

    public Privilege[] getInputDBLevelRequiredPriv() {
      return inputDBLevelRequiredPriv;
    }

    public Privilege[] getInputTableLevelRequiredPriv() {
      return inputTableLevelRequiredPriv;
    }

    public Privilege[] getInputColumnLevelRequiredPriv() {
      return inputColumnLevelRequiredPriv;
    }

    public Privilege[] getOutputUserLevelRequiredPriv() {
      return outputUserLevelRequiredPriv;
    }

    public Privilege[] getOutputDBLevelRequiredPriv() {
      return outputDBLevelRequiredPriv;
    }

    public Privilege[] getOutputTableLevelRequiredPriv() {
      return outputTableLevelRequiredPriv;
    }

    public Privilege[] getOutputColumnLevelRequiredPriv() {
      return outputColumnLevelRequiredPriv;
    }
  }
}
