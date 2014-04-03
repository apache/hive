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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;

/**
 * Mapping of operation to its required input and output privileges
 */
public class Operation2Privilege {

  private static class InOutPrivs {
    private final SQLPrivTypeGrant[] inputPrivs;
    private final SQLPrivTypeGrant[] outputPrivs;

    InOutPrivs(SQLPrivTypeGrant[] inputPrivs, SQLPrivTypeGrant[] outputPrivs) {
      this.inputPrivs = inputPrivs;
      this.outputPrivs = outputPrivs;
    }

    private SQLPrivTypeGrant[] getInputPrivs() {
      return inputPrivs;
    }

    private SQLPrivTypeGrant[] getOutputPrivs() {
      return outputPrivs;
    }
  }

  private static Map<HiveOperationType, InOutPrivs> op2Priv;

  private static SQLPrivTypeGrant[] OWNER_PRIV_AR = arr(SQLPrivTypeGrant.OWNER_PRIV);
  private static SQLPrivTypeGrant[] SEL_NOGRANT_AR = arr(SQLPrivTypeGrant.SELECT_NOGRANT);
  private static SQLPrivTypeGrant[] SEL_GRANT_AR = arr(SQLPrivTypeGrant.SELECT_WGRANT);
  private static SQLPrivTypeGrant[] ADMIN_PRIV_AR = arr(SQLPrivTypeGrant.ADMIN_PRIV);
  private static SQLPrivTypeGrant[] INS_NOGRANT_AR = arr(SQLPrivTypeGrant.INSERT_NOGRANT);
  private static SQLPrivTypeGrant[] DEL_NOGRANT_AR = arr(SQLPrivTypeGrant.DELETE_NOGRANT);
  private static SQLPrivTypeGrant[] OWNER_INS_SEL_DEL_NOGRANT_AR =
      arr(SQLPrivTypeGrant.OWNER_PRIV,
          SQLPrivTypeGrant.INSERT_NOGRANT,
          SQLPrivTypeGrant.DELETE_NOGRANT,
          SQLPrivTypeGrant.SELECT_NOGRANT);



  static {
    op2Priv = new HashMap<HiveOperationType, InOutPrivs>();

    op2Priv.put(HiveOperationType.EXPLAIN, new InOutPrivs(SEL_NOGRANT_AR,
        SEL_NOGRANT_AR)); //??

    op2Priv.put(HiveOperationType.CREATEDATABASE,
        new InOutPrivs(ADMIN_PRIV_AR, OWNER_INS_SEL_DEL_NOGRANT_AR));

    op2Priv.put(HiveOperationType.DROPDATABASE, new InOutPrivs(null, OWNER_PRIV_AR));
    // this should be database usage privilege once it is supported
    op2Priv.put(HiveOperationType.SWITCHDATABASE, new InOutPrivs(null, null));

    // lock operations not controlled for now
    op2Priv.put(HiveOperationType.LOCKDB, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.UNLOCKDB, new InOutPrivs(null, null));

    op2Priv.put(HiveOperationType.DROPTABLE, new InOutPrivs(OWNER_PRIV_AR, null));
    op2Priv.put(HiveOperationType.DESCTABLE, new InOutPrivs(SEL_NOGRANT_AR, null));
    op2Priv.put(HiveOperationType.SHOWPARTITIONS, new InOutPrivs(SEL_NOGRANT_AR, null));
    op2Priv.put(HiveOperationType.DESCFUNCTION, new InOutPrivs(null, null));

    // meta store check command - require admin priv
    op2Priv.put(HiveOperationType.MSCK, new InOutPrivs(ADMIN_PRIV_AR, null));


    //alter table commands require table ownership
    // There should not be output object, but just in case the table is incorrectly added
    // to output instead of input, adding owner requirement on output will catch that as well
    op2Priv.put(HiveOperationType.ALTERTABLE_ADDCOLS, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_REPLACECOLS, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAMECOL, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAMEPART, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAME, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_TOUCH, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_ARCHIVE, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_UNARCHIVE, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_PROPERTIES, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_SERIALIZER, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_PARTCOLTYPE, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_SERIALIZER, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_SERDEPROPERTIES, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_SERDEPROPERTIES, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_CLUSTER_SORT, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_BUCKETNUM, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_BUCKETNUM, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_PROTECTMODE, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_PROTECTMODE, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_FILEFORMAT, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_FILEFORMAT, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_LOCATION, new InOutPrivs(OWNER_PRIV_AR, OWNER_INS_SEL_DEL_NOGRANT_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_LOCATION, new InOutPrivs(OWNER_PRIV_AR, OWNER_INS_SEL_DEL_NOGRANT_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_MERGEFILES, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_MERGEFILES, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_SKEWED, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTBLPART_SKEWED_LOCATION, new InOutPrivs(OWNER_PRIV_AR, OWNER_INS_SEL_DEL_NOGRANT_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_COMPACT, new InOutPrivs(OWNER_PRIV_AR,  OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.TRUNCATETABLE, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));

    //table ownership for create/drop/alter index
    op2Priv.put(HiveOperationType.CREATEINDEX, new InOutPrivs(OWNER_PRIV_AR, OWNER_INS_SEL_DEL_NOGRANT_AR));
    op2Priv.put(HiveOperationType.DROPINDEX, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERINDEX_REBUILD, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERINDEX_PROPS, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));

    // require view ownership for alter/drop view
    op2Priv.put(HiveOperationType.ALTERVIEW_PROPERTIES, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.DROPVIEW_PROPERTIES, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERVIEW_RENAME, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.DROPVIEW, new InOutPrivs(OWNER_PRIV_AR, OWNER_PRIV_AR));

    op2Priv.put(HiveOperationType.ANALYZE_TABLE, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_NOGRANT, SQLPrivTypeGrant.INSERT_NOGRANT), null));
    op2Priv.put(HiveOperationType.SHOWDATABASES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWTABLES, new InOutPrivs(null, null));

    // operations that require insert/delete privileges
    op2Priv.put(HiveOperationType.ALTERTABLE_DROPPARTS, new InOutPrivs(DEL_NOGRANT_AR, null));
    // in alter-table-add-partition, the table is output, and location is input
    op2Priv.put(HiveOperationType.ALTERTABLE_ADDPARTS, new InOutPrivs(OWNER_INS_SEL_DEL_NOGRANT_AR, INS_NOGRANT_AR));

    // select with grant for exporting contents
    op2Priv.put(HiveOperationType.EXPORT, new InOutPrivs(SEL_GRANT_AR, null));
    op2Priv.put(HiveOperationType.IMPORT, new InOutPrivs(INS_NOGRANT_AR, null));

    // operations require select priv
    op2Priv.put(HiveOperationType.SHOWCOLUMNS, new InOutPrivs(SEL_NOGRANT_AR, null));
    op2Priv.put(HiveOperationType.SHOW_TABLESTATUS, new InOutPrivs(SEL_NOGRANT_AR, null));
    op2Priv.put(HiveOperationType.SHOW_TBLPROPERTIES, new InOutPrivs(SEL_NOGRANT_AR, null));
    op2Priv.put(HiveOperationType.CREATETABLE_AS_SELECT, new InOutPrivs(SEL_NOGRANT_AR, null));

    // QUERY,LOAD op can contain an insert & ovewrite, so require insert+delete privileges on output
    op2Priv.put(HiveOperationType.QUERY, new InOutPrivs(SEL_NOGRANT_AR,
        arr(SQLPrivTypeGrant.INSERT_NOGRANT, SQLPrivTypeGrant.DELETE_NOGRANT)));

    op2Priv.put(HiveOperationType.LOAD, new InOutPrivs(OWNER_INS_SEL_DEL_NOGRANT_AR,
        arr(SQLPrivTypeGrant.INSERT_NOGRANT, SQLPrivTypeGrant.DELETE_NOGRANT)));

    // show create table is more sensitive information, includes table properties etc
    // for now require select WITH GRANT
    op2Priv.put(HiveOperationType.SHOW_CREATETABLE, new InOutPrivs(SEL_GRANT_AR, null));

    // for now allow only create-view with 'select with grant'
    // the owner will also have select with grant privileges on new view
    op2Priv.put(HiveOperationType.CREATEVIEW, new InOutPrivs(SEL_GRANT_AR, null));

    op2Priv.put(HiveOperationType.SHOWFUNCTIONS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWINDEXES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWLOCKS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATEFUNCTION, new InOutPrivs(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.DROPFUNCTION, new InOutPrivs(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.CREATEMACRO, new InOutPrivs(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.DROPMACRO, new InOutPrivs(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.SHOW_COMPACTIONS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOW_TRANSACTIONS, new InOutPrivs(null, null));

    op2Priv.put(HiveOperationType.LOCKTABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.UNLOCKTABLE, new InOutPrivs(null, null));

    // require db ownership, if there is a file require SELECT , INSERT, and DELETE
    op2Priv.put(HiveOperationType.CREATETABLE,
        new InOutPrivs(OWNER_INS_SEL_DEL_NOGRANT_AR, OWNER_PRIV_AR));

    op2Priv.put(HiveOperationType.ALTERDATABASE, new InOutPrivs(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERDATABASE_OWNER, new InOutPrivs(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.DESCDATABASE, new InOutPrivs(null, null));

    // The following actions are authorized through SQLStdHiveAccessController,
    // and it is not using this privilege mapping, but it might make sense to move it here
    op2Priv.put(HiveOperationType.CREATEROLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPROLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.GRANT_PRIVILEGE, new InOutPrivs(null,
        null));
    op2Priv.put(HiveOperationType.REVOKE_PRIVILEGE, new InOutPrivs(null,
        null));
    op2Priv.put(HiveOperationType.SHOW_GRANT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.GRANT_ROLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.REVOKE_ROLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOW_ROLES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOW_ROLE_GRANT, new InOutPrivs(null,
        null));
    op2Priv.put(HiveOperationType.SHOW_ROLE_PRINCIPALS, new InOutPrivs(null,
        null));


  }

  /**
   * Convenience method so that creation of this array in InOutPrivs constructor
   * is not too verbose
   *
   * @param grantList
   * @return grantList
   */
  private static SQLPrivTypeGrant[] arr(SQLPrivTypeGrant... grantList) {
    return grantList;
  }

  public static SQLPrivTypeGrant[] getInputPrivs(HiveOperationType opType) {
    return op2Priv.get(opType).getInputPrivs();
  }

  public static SQLPrivTypeGrant[] getOutputPrivs(HiveOperationType opType) {
    return op2Priv.get(opType).getOutputPrivs();
  }

  // for unit tests
  public static Set<HiveOperationType> getOperationTypes() {
    return op2Priv.keySet();
  }

}
