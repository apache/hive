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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;

import com.google.common.base.Preconditions;

/**
 * Mapping of operation to its required input and output privileges
 */
public class Operation2Privilege {

  public enum IOType {
    INPUT, OUTPUT
  };

  private static class PrivRequirement {

    private final SQLPrivTypeGrant[] reqPrivs;
    // The following fields specify the criteria on objects for this priv to be required
    private final IOType ioType;
    private final HivePrivObjectActionType actionType;
    private final HivePrivilegeObjectType objectType;


    private PrivRequirement(SQLPrivTypeGrant[] privs, IOType ioType) {
      this(privs, ioType, null);
    }

    private PrivRequirement(SQLPrivTypeGrant[] privs, IOType ioType,
        HivePrivObjectActionType actionType) {
      this(privs, ioType, actionType, null);
    }

    private PrivRequirement(SQLPrivTypeGrant[] privs, HivePrivilegeObjectType objectType) {
      this(privs, null, null, objectType);
    }

    private PrivRequirement(SQLPrivTypeGrant[] privs, IOType ioType,
        HivePrivObjectActionType actionType, HivePrivilegeObjectType objectType) {
      this.reqPrivs = privs;
      this.ioType = ioType;
      this.actionType = actionType;
      this.objectType = objectType;
    }


    /**
     * Utility function that takes a input and output privilege objects
     * @param inGrant
     * @param outGrant
     * @return
     */
    static List<PrivRequirement> newIOPrivRequirement(SQLPrivTypeGrant[] inGrants,
        SQLPrivTypeGrant[] outGrants) {
      List<PrivRequirement> privReqs = new ArrayList<PrivRequirement>();
      privReqs.add(new PrivRequirement(inGrants, IOType.INPUT));
      privReqs.add(new PrivRequirement(outGrants, IOType.OUTPUT));
      return privReqs;
    }

    /**
     * Utility function that converts PrivRequirement array into list
     * @param privs
     * @return
     */
    static List<PrivRequirement> newPrivRequirementList(PrivRequirement... privs) {
      return new ArrayList<PrivRequirement>(Arrays.asList(privs));
    }

    private SQLPrivTypeGrant[] getReqPrivs() {
      return reqPrivs;
    }

    private IOType getIOType() {
      return ioType;
    }

    private HivePrivObjectActionType getActionType() {
      return actionType;
    }

    public HivePrivilegeObjectType getObjectType() {
      return objectType;
    }

  }

  private static Map<HiveOperationType, List<PrivRequirement>> op2Priv;

  private static SQLPrivTypeGrant[] OWNER_PRIV_AR = arr(SQLPrivTypeGrant.OWNER_PRIV);
  private static SQLPrivTypeGrant[] SEL_NOGRANT_AR = arr(SQLPrivTypeGrant.SELECT_NOGRANT);
  private static SQLPrivTypeGrant[] SEL_GRANT_AR = arr(SQLPrivTypeGrant.SELECT_WGRANT);
  private static SQLPrivTypeGrant[] ADMIN_PRIV_AR = arr(SQLPrivTypeGrant.ADMIN_PRIV);
  private static SQLPrivTypeGrant[] INS_NOGRANT_AR = arr(SQLPrivTypeGrant.INSERT_NOGRANT);
  private static SQLPrivTypeGrant[] DEL_NOGRANT_AR = arr(SQLPrivTypeGrant.DELETE_NOGRANT);
  private static SQLPrivTypeGrant[] UPD_NOGRANT_AR = arr(SQLPrivTypeGrant.UPDATE_NOGRANT);
  private static SQLPrivTypeGrant[] OWNER_INS_SEL_DEL_NOGRANT_AR =
      arr(SQLPrivTypeGrant.OWNER_PRIV,
          SQLPrivTypeGrant.INSERT_NOGRANT,
          SQLPrivTypeGrant.DELETE_NOGRANT,
          SQLPrivTypeGrant.SELECT_NOGRANT);



  static {
    op2Priv = new HashMap<HiveOperationType, List<PrivRequirement>>();

    op2Priv.put(HiveOperationType.EXPLAIN, PrivRequirement.newIOPrivRequirement
(SEL_NOGRANT_AR,
        SEL_NOGRANT_AR)); //??

    op2Priv.put(HiveOperationType.CREATEDATABASE, PrivRequirement.newPrivRequirementList(
        new PrivRequirement(OWNER_INS_SEL_DEL_NOGRANT_AR, HivePrivilegeObjectType.DFS_URI),
        new PrivRequirement(OWNER_INS_SEL_DEL_NOGRANT_AR, HivePrivilegeObjectType.LOCAL_URI)));

    op2Priv.put(HiveOperationType.DROPDATABASE, PrivRequirement.newIOPrivRequirement
(null, OWNER_PRIV_AR));
    // this should be database usage privilege once it is supported
    op2Priv.put(HiveOperationType.SWITCHDATABASE, PrivRequirement.newIOPrivRequirement
(null, null));

    // lock operations not controlled for now
    op2Priv.put(HiveOperationType.LOCKDB, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.UNLOCKDB, PrivRequirement.newIOPrivRequirement
(null, null));

    op2Priv.put(HiveOperationType.DROPTABLE, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, null));
    op2Priv.put(HiveOperationType.DESCTABLE, PrivRequirement.newIOPrivRequirement
(SEL_NOGRANT_AR, null));
    op2Priv.put(HiveOperationType.SHOWPARTITIONS, PrivRequirement.newIOPrivRequirement
(SEL_NOGRANT_AR, null));
    op2Priv.put(HiveOperationType.DESCFUNCTION, PrivRequirement.newIOPrivRequirement
(null, null));

    // meta store check command - require admin priv
    op2Priv.put(HiveOperationType.MSCK, PrivRequirement.newIOPrivRequirement
(ADMIN_PRIV_AR, null));


    //alter table commands require table ownership
    // There should not be output object, but just in case the table is incorrectly added
    // to output instead of input, adding owner requirement on output will catch that as well
    op2Priv.put(HiveOperationType.ALTERTABLE_ADDCOLS, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_REPLACECOLS, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAMECOL, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAMEPART, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAME, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_UPDATETABLESTATS, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_UPDATEPARTSTATS, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_TOUCH, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_ARCHIVE, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_UNARCHIVE, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_PROPERTIES, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_SERIALIZER, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_PARTCOLTYPE, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_SERIALIZER, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_SERDEPROPERTIES, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_SERDEPROPERTIES, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_CLUSTER_SORT, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_BUCKETNUM, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_BUCKETNUM, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_PROTECTMODE, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_PROTECTMODE, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_FILEFORMAT, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_FILEFORMAT, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_LOCATION, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_INS_SEL_DEL_NOGRANT_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_LOCATION, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_INS_SEL_DEL_NOGRANT_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_MERGEFILES, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERPARTITION_MERGEFILES, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_SKEWED, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERTBLPART_SKEWED_LOCATION, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_INS_SEL_DEL_NOGRANT_AR));
    op2Priv.put(HiveOperationType.ALTERTABLE_COMPACT, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR,  OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.TRUNCATETABLE, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));

    //table ownership for create/drop/alter index
    op2Priv.put(HiveOperationType.CREATEINDEX, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_INS_SEL_DEL_NOGRANT_AR));
    op2Priv.put(HiveOperationType.DROPINDEX, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERINDEX_REBUILD, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERINDEX_PROPS, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));

    // require view ownership for alter/drop view
    op2Priv.put(HiveOperationType.ALTERVIEW_PROPERTIES, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.DROPVIEW_PROPERTIES, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERVIEW_RENAME, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERVIEW_AS, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));
    op2Priv.put(HiveOperationType.DROPVIEW, PrivRequirement.newIOPrivRequirement
(OWNER_PRIV_AR, OWNER_PRIV_AR));

    op2Priv.put(HiveOperationType.ANALYZE_TABLE, PrivRequirement.newIOPrivRequirement
(arr(SQLPrivTypeGrant.SELECT_NOGRANT, SQLPrivTypeGrant.INSERT_NOGRANT), null));
    op2Priv.put(HiveOperationType.SHOWDATABASES, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.SHOWTABLES, PrivRequirement.newIOPrivRequirement
(null, null));

    // operations that require insert/delete privileges
    op2Priv.put(HiveOperationType.ALTERTABLE_DROPPARTS, PrivRequirement.newIOPrivRequirement
(DEL_NOGRANT_AR, null));
    // in alter-table-add-partition, the table is output, and location is input
    op2Priv.put(HiveOperationType.ALTERTABLE_ADDPARTS, PrivRequirement.newIOPrivRequirement
(OWNER_INS_SEL_DEL_NOGRANT_AR, INS_NOGRANT_AR));

    // select with grant for exporting contents
    op2Priv.put(HiveOperationType.EXPORT, PrivRequirement.newIOPrivRequirement
(SEL_GRANT_AR, OWNER_INS_SEL_DEL_NOGRANT_AR));
    op2Priv.put(HiveOperationType.IMPORT, PrivRequirement.newIOPrivRequirement
(OWNER_INS_SEL_DEL_NOGRANT_AR, INS_NOGRANT_AR));

    // operations require select priv
    op2Priv.put(HiveOperationType.SHOWCOLUMNS, PrivRequirement.newIOPrivRequirement
(SEL_NOGRANT_AR, null));
    op2Priv.put(HiveOperationType.SHOW_TABLESTATUS, PrivRequirement.newIOPrivRequirement
(SEL_NOGRANT_AR, null));
    op2Priv.put(HiveOperationType.SHOW_TBLPROPERTIES, PrivRequirement.newIOPrivRequirement
(SEL_NOGRANT_AR, null));
    op2Priv.put(HiveOperationType.CREATETABLE_AS_SELECT, PrivRequirement.newPrivRequirementList(
        new PrivRequirement(SEL_NOGRANT_AR, IOType.INPUT),
        new PrivRequirement(OWNER_PRIV_AR, HivePrivilegeObjectType.DATABASE)));

    // QUERY,LOAD op can contain an insert & overwrite,
    // require delete privilege if this is an insert-overwrite
    op2Priv.put(HiveOperationType.QUERY,
        arr(
            new PrivRequirement(SEL_NOGRANT_AR, IOType.INPUT),
            new PrivRequirement(INS_NOGRANT_AR, IOType.OUTPUT, HivePrivObjectActionType.INSERT),
            new PrivRequirement(
                arr(SQLPrivTypeGrant.INSERT_NOGRANT, SQLPrivTypeGrant.DELETE_NOGRANT),
                IOType.OUTPUT,
                HivePrivObjectActionType.INSERT_OVERWRITE),
            new PrivRequirement(DEL_NOGRANT_AR, IOType.OUTPUT, HivePrivObjectActionType.DELETE),
            new PrivRequirement(UPD_NOGRANT_AR, IOType.OUTPUT, HivePrivObjectActionType.UPDATE),
            new PrivRequirement(INS_NOGRANT_AR, IOType.OUTPUT, HivePrivObjectActionType.OTHER)
            )
        );

    op2Priv.put(HiveOperationType.LOAD, PrivRequirement.newIOPrivRequirement
(OWNER_INS_SEL_DEL_NOGRANT_AR,
        arr(SQLPrivTypeGrant.INSERT_NOGRANT, SQLPrivTypeGrant.DELETE_NOGRANT)));

    // show create table is more sensitive information, includes table properties etc
    // for now require select WITH GRANT
    op2Priv.put(HiveOperationType.SHOW_CREATETABLE, PrivRequirement.newIOPrivRequirement
(SEL_GRANT_AR, null));

    // for now allow only create-view with 'select with grant'
    // the owner will also have select with grant privileges on new view
    op2Priv.put(HiveOperationType.CREATEVIEW, PrivRequirement.newPrivRequirementList(
        new PrivRequirement(SEL_GRANT_AR, IOType.INPUT),
        new PrivRequirement(OWNER_PRIV_AR, HivePrivilegeObjectType.DATABASE)));

    op2Priv.put(HiveOperationType.SHOWFUNCTIONS, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.SHOWINDEXES, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.SHOWLOCKS, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.CREATEFUNCTION, PrivRequirement.newIOPrivRequirement
(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.DROPFUNCTION, PrivRequirement.newIOPrivRequirement
(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.CREATEMACRO, PrivRequirement.newIOPrivRequirement
(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.DROPMACRO, PrivRequirement.newIOPrivRequirement
(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.SHOW_COMPACTIONS, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.SHOW_TRANSACTIONS, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.SHOWCONF, PrivRequirement.newIOPrivRequirement
(null, null));

    op2Priv.put(HiveOperationType.LOCKTABLE, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.UNLOCKTABLE, PrivRequirement.newIOPrivRequirement
(null, null));

    // require db ownership, if there is a file require SELECT , INSERT, and DELETE
    op2Priv.put(HiveOperationType.CREATETABLE, PrivRequirement.newPrivRequirementList(
        new PrivRequirement(OWNER_INS_SEL_DEL_NOGRANT_AR, IOType.INPUT),
        new PrivRequirement(OWNER_PRIV_AR, HivePrivilegeObjectType.DATABASE)));

    op2Priv.put(HiveOperationType.ALTERDATABASE, PrivRequirement.newIOPrivRequirement
(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.ALTERDATABASE_OWNER, PrivRequirement.newIOPrivRequirement
(null, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.DESCDATABASE, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.DFS, PrivRequirement.newIOPrivRequirement
(ADMIN_PRIV_AR, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.RESET, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.COMPILE, PrivRequirement.newIOPrivRequirement
(ADMIN_PRIV_AR, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.ADD, PrivRequirement.newIOPrivRequirement
(ADMIN_PRIV_AR, ADMIN_PRIV_AR));
    op2Priv.put(HiveOperationType.DELETE, PrivRequirement.newIOPrivRequirement
(ADMIN_PRIV_AR, ADMIN_PRIV_AR));
    // set command is currently not authorized through the API
    op2Priv.put(HiveOperationType.SET, PrivRequirement.newIOPrivRequirement
(null, null));

    // The following actions are authorized through SQLStdHiveAccessController,
    // and it is not using this privilege mapping, but it might make sense to move it here
    op2Priv.put(HiveOperationType.CREATEROLE, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.DROPROLE, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.GRANT_PRIVILEGE, PrivRequirement.newIOPrivRequirement
(null,
        null));
    op2Priv.put(HiveOperationType.REVOKE_PRIVILEGE, PrivRequirement.newIOPrivRequirement
(null,
        null));
    op2Priv.put(HiveOperationType.SHOW_GRANT, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.GRANT_ROLE, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.REVOKE_ROLE, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.SHOW_ROLES, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.SHOW_ROLE_GRANT, PrivRequirement.newIOPrivRequirement
(null, null));
    op2Priv.put(HiveOperationType.SHOW_ROLE_PRINCIPALS,
        PrivRequirement.newIOPrivRequirement(null, null));
    op2Priv.put(HiveOperationType.GET_CATALOGS, PrivRequirement.newIOPrivRequirement(null, null));
    op2Priv.put(HiveOperationType.GET_SCHEMAS, PrivRequirement.newIOPrivRequirement(null, null));
    op2Priv.put(HiveOperationType.GET_TABLES, PrivRequirement.newIOPrivRequirement(null, null));
    op2Priv.put(HiveOperationType.GET_FUNCTIONS, PrivRequirement.newIOPrivRequirement(null, null));
    op2Priv.put(HiveOperationType.GET_TABLETYPES, PrivRequirement.newIOPrivRequirement(null, null));
    op2Priv.put(HiveOperationType.GET_TYPEINFO, PrivRequirement.newIOPrivRequirement(null, null));
    op2Priv.put(HiveOperationType.GET_COLUMNS,
        PrivRequirement.newIOPrivRequirement(SEL_NOGRANT_AR, null));

  }

  /**
   * Convenience method so that creation of this array in PrivRequirement constructor
   * is not too verbose
   *
   * @param grantList
   * @return grantList
   */
  private static SQLPrivTypeGrant[] arr(SQLPrivTypeGrant... grantList) {
    return grantList;
  }

  /**
   * Convenience method so that creation of list of PrivRequirement is not too verbose
   * @param privReqList
   * @return
   */
  private static List<PrivRequirement> arr(PrivRequirement... privReqList){
    return Arrays.asList(privReqList);
  }

  /**
   * Get the privileges required for this operation (hiveOpType) on hive object (hObj) when its
   * IOType is ioType. Looks at the action type in hObj to find privileges that are applicable
   * to that action.
   *
   * @param hiveOpType
   * @param hObj
   * @param ioType
   * @return
   */
  public static RequiredPrivileges getRequiredPrivs(HiveOperationType hiveOpType,
      HivePrivilegeObject hObj, IOType ioType) {
    List<PrivRequirement> opPrivs = op2Priv.get(hiveOpType);
    Preconditions.checkNotNull(opPrivs, "Privileges for " + hiveOpType + " are null");
    RequiredPrivileges reqPrivs = new RequiredPrivileges();

    // Find the PrivRequirements that match on IOType, ActionType, and HivePrivilegeObjectType add
    // the privilege required to reqPrivs
    for (PrivRequirement opPriv : opPrivs) {
      if (opPriv.getIOType() != null && opPriv.getIOType() != ioType) {
        continue;
      }
      if (opPriv.getActionType() != null && opPriv.getActionType() != hObj.getActionType()) {
        continue;
      }
      if (opPriv.getObjectType() != null && opPriv.getObjectType() != hObj.getType()) {
        continue;
      }
      reqPrivs.addAll(opPriv.getReqPrivs());
    }

    return reqPrivs;
  }

  // for unit tests
  public static Set<HiveOperationType> getOperationTypes() {
    return op2Priv.keySet();
  }

}
