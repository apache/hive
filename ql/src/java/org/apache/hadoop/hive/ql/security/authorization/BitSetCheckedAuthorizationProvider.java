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

package org.apache.hadoop.hive.ql.security.authorization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

public abstract class BitSetCheckedAuthorizationProvider extends
    HiveAuthorizationProviderBase {

  static class BitSetChecker {

    boolean[] inputCheck = null;
    boolean[] outputCheck = null;

    public static BitSetChecker getBitSetChecker(Privilege[] inputRequiredPriv,
        Privilege[] outputRequiredPriv) {
      BitSetChecker checker = new BitSetChecker();
      if (inputRequiredPriv != null) {
        checker.inputCheck = new boolean[inputRequiredPriv.length];
        for (int i = 0; i < checker.inputCheck.length; i++) {
          checker.inputCheck[i] = false;
        }
      }
      if (outputRequiredPriv != null) {
        checker.outputCheck = new boolean[outputRequiredPriv.length];
        for (int i = 0; i < checker.outputCheck.length; i++) {
          checker.outputCheck[i] = false;
        }
      }

      return checker;
    }

  }

  @Override
  public void authorizeDbLevelOperations(Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv,
      Collection<ReadEntity> inputs, Collection<WriteEntity> outputs) throws HiveException, AuthorizationException {

    BitSetChecker checker = BitSetChecker.getBitSetChecker(inputRequiredPriv,
        outputRequiredPriv);
    boolean[] inputCheck = checker.inputCheck;
    boolean[] outputCheck = checker.outputCheck;

    authorizeUserPriv(inputRequiredPriv, inputCheck, outputRequiredPriv,
        outputCheck);
    checkAndThrowAuthorizationException(inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck, null, null, null, null);
  }

  @Override
  public void authorize(Database db, Privilege[] inputRequiredPriv,
      Privilege[] outputRequiredPriv) throws HiveException, AuthorizationException {

    BitSetChecker checker = BitSetChecker.getBitSetChecker(inputRequiredPriv,
        outputRequiredPriv);
    boolean[] inputCheck = checker.inputCheck;
    boolean[] outputCheck = checker.outputCheck;

    authorizeUserAndDBPriv(db, inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck);

    checkAndThrowAuthorizationException(inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck, db.getName(), null, null, null);
  }

  @Override
  public void authorize(Table table, Privilege[] inputRequiredPriv,
      Privilege[] outputRequiredPriv) throws HiveException {

    BitSetChecker checker = BitSetChecker.getBitSetChecker(inputRequiredPriv,
        outputRequiredPriv);
    boolean[] inputCheck = checker.inputCheck;
    boolean[] outputCheck = checker.outputCheck;

    authorizeUserDBAndTable(table, inputRequiredPriv,
        outputRequiredPriv, inputCheck, outputCheck);
    checkAndThrowAuthorizationException(inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck, table.getDbName(), table.getTableName(),
        null, null);
  }

  @Override
  public void authorize(Partition part, Privilege[] inputRequiredPriv,
      Privilege[] outputRequiredPriv) throws HiveException {

    //if the partition does not have partition level privilege, go to table level.
    Table table = part.getTable();
    if (table.getParameters().get("PARTITION_LEVEL_PRIVILEGE") == null || ("FALSE"
        .equalsIgnoreCase(table.getParameters().get(
            "PARTITION_LEVEL_PRIVILEGE")))) {
      this.authorize(part.getTable(), inputRequiredPriv, outputRequiredPriv);
      return;
    }

    BitSetChecker checker = BitSetChecker.getBitSetChecker(inputRequiredPriv,
        outputRequiredPriv);
    boolean[] inputCheck = checker.inputCheck;
    boolean[] outputCheck = checker.outputCheck;

    if (authorizeUserDbAndPartition(part, inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck)){
      return;
    }

    checkAndThrowAuthorizationException(inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck, part.getTable().getDbName(), part
            .getTable().getTableName(), part.getName(), null);
  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns,
      Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv)
      throws HiveException {

    BitSetChecker checker = BitSetChecker.getBitSetChecker(inputRequiredPriv,
        outputRequiredPriv);
    boolean[] inputCheck = checker.inputCheck;
    boolean[] outputCheck = checker.outputCheck;

    String partName = null;
    List<String> partValues = null;
    if (part != null
        && (table.getParameters().get("PARTITION_LEVEL_PRIVILEGE") != null && ("TRUE"
            .equalsIgnoreCase(table.getParameters().get(
                "PARTITION_LEVEL_PRIVILEGE"))))) {
      partName = part.getName();
      partValues = part.getValues();
    }

    if (partValues == null) {
      if (authorizeUserDBAndTable(table, inputRequiredPriv, outputRequiredPriv,
          inputCheck, outputCheck)) {
        return;
      }
    } else {
      if (authorizeUserDbAndPartition(part, inputRequiredPriv,
          outputRequiredPriv, inputCheck, outputCheck)) {
        return;
      }
    }

    for (String col : columns) {

      BitSetChecker checker2 = BitSetChecker.getBitSetChecker(
          inputRequiredPriv, outputRequiredPriv);
      boolean[] inputCheck2 = checker2.inputCheck;
      boolean[] outputCheck2 = checker2.outputCheck;

      PrincipalPrivilegeSet partColumnPrivileges = hive_db
          .get_privilege_set(HiveObjectType.COLUMN, table.getDbName(), table.getTableName(),
              partValues, col, this.getAuthenticator().getUserName(), this
                  .getAuthenticator().getGroupNames());

      authorizePrivileges(partColumnPrivileges, inputRequiredPriv, inputCheck2,
          outputRequiredPriv, outputCheck2);

      if (inputCheck2 != null) {
        booleanArrayOr(inputCheck2, inputCheck);
      }
      if (outputCheck2 != null) {
        booleanArrayOr(inputCheck2, inputCheck);
      }

      checkAndThrowAuthorizationException(inputRequiredPriv,
          outputRequiredPriv, inputCheck2, outputCheck2, table.getDbName(),
          table.getTableName(), partName, col);
    }
  }

  protected boolean authorizeUserPriv(Privilege[] inputRequiredPriv,
      boolean[] inputCheck, Privilege[] outputRequiredPriv,
      boolean[] outputCheck) throws HiveException {
    PrincipalPrivilegeSet privileges = hive_db.get_privilege_set(
        HiveObjectType.GLOBAL, null, null, null, null, this.getAuthenticator()
            .getUserName(), this.getAuthenticator().getGroupNames());
    return authorizePrivileges(privileges, inputRequiredPriv, inputCheck,
        outputRequiredPriv, outputCheck);
  }

  /**
   * Check privileges on User and DB. This is used before doing a check on
   * table/partition objects, first check the user and DB privileges. If it
   * passed on this check, no need to check against the table/partition hive
   * object.
   *
   * @param db
   * @param inputRequiredPriv
   * @param outputRequiredPriv
   * @param inputCheck
   * @param outputCheck
   * @return true if the check on user and DB privilege passed, which means no
   *         need for privilege check on concrete hive objects.
   * @throws HiveException
   */
  private boolean authorizeUserAndDBPriv(Database db,
      Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv,
      boolean[] inputCheck, boolean[] outputCheck) throws HiveException {
    if (authorizeUserPriv(inputRequiredPriv, inputCheck, outputRequiredPriv,
        outputCheck)) {
      return true;
    }

    PrincipalPrivilegeSet dbPrivileges = hive_db.get_privilege_set(
        HiveObjectType.DATABASE, db.getName(), null, null, null, this
            .getAuthenticator().getUserName(), this.getAuthenticator()
            .getGroupNames());

    if (authorizePrivileges(dbPrivileges, inputRequiredPriv, inputCheck,
        outputRequiredPriv, outputCheck)) {
      return true;
    }

    return false;
  }

  /**
   * Check privileges on User, DB and table objects.
   *
   * @param table
   * @param inputRequiredPriv
   * @param outputRequiredPriv
   * @param inputCheck
   * @param outputCheck
   * @return true if the check passed
   * @throws HiveException
   */
  private boolean authorizeUserDBAndTable(Table table,
      Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv,
      boolean[] inputCheck, boolean[] outputCheck) throws HiveException {

    if (authorizeUserAndDBPriv(hive_db.getDatabase(table.getCatName(), table.getDbName()),
        inputRequiredPriv, outputRequiredPriv, inputCheck, outputCheck)) {
      return true;
    }

    PrincipalPrivilegeSet tablePrivileges = hive_db.get_privilege_set(
        HiveObjectType.TABLE, table.getDbName(), table.getTableName(), null,
        null, this.getAuthenticator().getUserName(), this.getAuthenticator()
            .getGroupNames());

    if (authorizePrivileges(tablePrivileges, inputRequiredPriv, inputCheck,
        outputRequiredPriv, outputCheck)) {
      return true;
    }

    return false;
  }

  /**
   * Check privileges on User, DB and table/Partition objects.
   *
   * @param part
   * @param inputRequiredPriv
   * @param outputRequiredPriv
   * @param inputCheck
   * @param outputCheck
   * @return true if the check passed
   * @throws HiveException
   */
  private boolean authorizeUserDbAndPartition(Partition part,
      Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv,
      boolean[] inputCheck, boolean[] outputCheck) throws HiveException {

    if (authorizeUserAndDBPriv(
        hive_db.getDatabase(part.getTable().getCatName(), part.getTable().getDbName()),
        inputRequiredPriv, outputRequiredPriv, inputCheck, outputCheck)) {
      return true;
    }

    PrincipalPrivilegeSet partPrivileges = part.getTPartition().getPrivileges();
    if (partPrivileges == null) {
      partPrivileges = hive_db.get_privilege_set(HiveObjectType.PARTITION, part
          .getTable().getDbName(), part.getTable().getTableName(), part
          .getValues(), null, this.getAuthenticator().getUserName(), this
          .getAuthenticator().getGroupNames());
    }

    if (authorizePrivileges(partPrivileges, inputRequiredPriv, inputCheck,
        outputRequiredPriv, outputCheck)) {
      return true;
    }

    return false;
  }

  protected boolean authorizePrivileges(PrincipalPrivilegeSet privileges,
      Privilege[] inputPriv, boolean[] inputCheck, Privilege[] outputPriv,
      boolean[] outputCheck) throws HiveException {

    boolean pass = true;
    if (inputPriv != null) {
      pass = pass && matchPrivs(inputPriv, privileges, inputCheck);
    }
    if (outputPriv != null) {
      pass = pass && matchPrivs(outputPriv, privileges, outputCheck);
    }
    return pass;
  }

  /**
   * try to match an array of privileges from user/groups/roles grants.
   *
   */
  private boolean matchPrivs(Privilege[] inputPriv,
      PrincipalPrivilegeSet privileges, boolean[] check) {

    if (inputPriv == null) {
      return true;
    }

    if (privileges == null) {
      return false;
    }

    /*
     * user grants
     */
    Set<String> privSet = new HashSet<String>();
    if (privileges.getUserPrivileges() != null
        && privileges.getUserPrivileges().size() > 0) {
      Collection<List<PrivilegeGrantInfo>> privCollection = privileges.getUserPrivileges().values();

      List<String> userPrivs = getPrivilegeStringList(privCollection);
      if (userPrivs != null && userPrivs.size() > 0) {
        for (String priv : userPrivs) {
          if (priv == null || priv.trim().equals("")) {
            continue;
          }
            if (priv.equalsIgnoreCase(Privilege.ALL.toString())) {
              setBooleanArray(check, true);
              return true;
            }
            privSet.add(priv.toLowerCase());
        }
      }
    }

    /*
     * group grants
     */
    if (privileges.getGroupPrivileges() != null
        && privileges.getGroupPrivileges().size() > 0) {
      Collection<List<PrivilegeGrantInfo>> groupPrivCollection = privileges
          .getGroupPrivileges().values();
      List<String> groupPrivs = getPrivilegeStringList(groupPrivCollection);
      if (groupPrivs != null && groupPrivs.size() > 0) {
        for (String priv : groupPrivs) {
          if (priv == null || priv.trim().equals("")) {
            continue;
          }
          if (priv.equalsIgnoreCase(Privilege.ALL.toString())) {
            setBooleanArray(check, true);
            return true;
          }
          privSet.add(priv.toLowerCase());
        }
      }
    }

    /*
     * roles grants
     */
    if (privileges.getRolePrivileges() != null
        && privileges.getRolePrivileges().size() > 0) {
      Collection<List<PrivilegeGrantInfo>> rolePrivsCollection = privileges
          .getRolePrivileges().values();
      ;
      List<String> rolePrivs = getPrivilegeStringList(rolePrivsCollection);
      if (rolePrivs != null && rolePrivs.size() > 0) {
        for (String priv : rolePrivs) {
          if (priv == null || priv.trim().equals("")) {
            continue;
          }
          if (priv.equalsIgnoreCase(Privilege.ALL.toString())) {
            setBooleanArray(check, true);
            return true;
          }
          privSet.add(priv.toLowerCase());
        }
      }
    }

    for (int i = 0; i < inputPriv.length; i++) {
      String toMatch = inputPriv[i].toString();
      if (!check[i]) {
        check[i] = privSet.contains(toMatch.toLowerCase());
      }
    }

    return firstFalseIndex(check) <0;
  }

  private List<String> getPrivilegeStringList(
      Collection<List<PrivilegeGrantInfo>> privCollection) {
    List<String> userPrivs = new ArrayList<String>();
    if (privCollection!= null && privCollection.size()>0) {
      for (List<PrivilegeGrantInfo> grantList : privCollection) {
        if (grantList == null){
          continue;
        }
        for (int i = 0; i < grantList.size(); i++) {
          PrivilegeGrantInfo grant = grantList.get(i);
          userPrivs.add(grant.getPrivilege());
        }
      }
    }
    return userPrivs;
  }

  private static void setBooleanArray(boolean[] check, boolean b) {
    for (int i = 0; i < check.length; i++) {
      check[i] = b;
    }
  }

  private static void booleanArrayOr(boolean[] output, boolean[] input) {
    for (int i = 0; i < output.length && i < input.length; i++) {
      output[i] = output[i] || input[i];
    }
  }

  private void checkAndThrowAuthorizationException(
      Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv,
      boolean[] inputCheck, boolean[] outputCheck,String dbName,
      String tableName, String partitionName, String columnName) {

    String hiveObject = "{ ";
    if (dbName != null) {
      hiveObject = hiveObject + "database:" + dbName;
    }
    if (tableName != null) {
      hiveObject = hiveObject + ", table:" + tableName;
    }
    if (partitionName != null) {
      hiveObject = hiveObject + ", partitionName:" + partitionName;
    }
    if (columnName != null) {
      hiveObject = hiveObject + ", columnName:" + columnName;
    }
    hiveObject = hiveObject + "}";

    if (inputCheck != null) {
      int input = this.firstFalseIndex(inputCheck);
      if (input >= 0) {
        throw new AuthorizationException("No privilege '"
            + inputRequiredPriv[input].toString() + "' found for inputs "
            + hiveObject);
      }
    }

    if (outputCheck != null) {
      int output = this.firstFalseIndex(outputCheck);
      if (output >= 0) {
        throw new AuthorizationException("No privilege '"
            + outputRequiredPriv[output].toString() + "' found for outputs "
            + hiveObject);
      }
    }
  }

  private int firstFalseIndex(boolean[] inputCheck) {
    if (inputCheck != null) {
      for (int i = 0; i < inputCheck.length; i++) {
        if (!inputCheck[i]) {
          return i;
        }
      }
    }
    return -1;
  }
}
