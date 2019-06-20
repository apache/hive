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
package org.apache.hadoop.hive.ql.parse.authorization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.privilege.CreateRoleDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.DropRoleDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.GrantDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.GrantRoleDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrivilegeDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.RevokeDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.RevokeRoleDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.SetRoleDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.ShowCurrentRoleDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.ShowGrantDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.ShowPrincipalsDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.ShowRoleGrantDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.ShowRolesDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeRegistry;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeType;
import org.apache.hadoop.hive.ql.session.SessionState;
/**
 * Default implementation of HiveAuthorizationTaskFactory
 */
public class HiveAuthorizationTaskFactoryImpl implements HiveAuthorizationTaskFactory {

  // Assumes one instance of this + single-threaded compilation for each query.
  private final Hive db;

    public HiveAuthorizationTaskFactoryImpl(HiveConf conf, Hive db) {
    this.db = db;
  }

  @Override
  public Task<? extends Serializable> createCreateRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) {
    String roleName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
    CreateRoleDesc createRoleDesc = new CreateRoleDesc(roleName);
    return TaskFactory.get(new DDLWork(inputs, outputs, createRoleDesc));
  }
  @Override
  public Task<? extends Serializable> createDropRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) {
    String roleName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
    DropRoleDesc dropRoleDesc = new DropRoleDesc(roleName);
    return TaskFactory.get(new DDLWork(inputs, outputs, dropRoleDesc));
  }
  @Override
  public Task<? extends Serializable> createShowRoleGrantTask(ASTNode ast, Path resultFile,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) {
    ASTNode child = (ASTNode) ast.getChild(0);
    PrincipalType principalType = PrincipalType.USER;
    switch (child.getType()) {
    case HiveParser.TOK_USER:
      principalType = PrincipalType.USER;
      break;
    case HiveParser.TOK_GROUP:
      principalType = PrincipalType.GROUP;
      break;
    case HiveParser.TOK_ROLE:
      principalType = PrincipalType.ROLE;
      break;
    }
    String principalName = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
    ShowRoleGrantDesc showRoleGrantDesc = new ShowRoleGrantDesc(principalName, principalType, resultFile.toString());
    return TaskFactory.get(new DDLWork(inputs, outputs, showRoleGrantDesc));
  }
  @Override
  public Task<? extends Serializable> createGrantTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {
    List<PrivilegeDesc> privilegeDesc = analyzePrivilegeListDef(
        (ASTNode) ast.getChild(0));
    List<PrincipalDesc> principalDesc = AuthorizationParseUtils.analyzePrincipalListDef(
        (ASTNode) ast.getChild(1));
    boolean grantOption = false;
    PrivilegeObjectDesc privilegeObj = null;

    if (ast.getChildCount() > 2) {
      for (int i = 2; i < ast.getChildCount(); i++) {
        ASTNode astChild = (ASTNode) ast.getChild(i);
        if (astChild.getType() == HiveParser.TOK_GRANT_WITH_OPTION) {
          grantOption = true;
        } else if (astChild.getType() == HiveParser.TOK_PRIV_OBJECT) {
          privilegeObj = analyzePrivilegeObject(astChild, outputs);
        }
      }
    }

    String userName = SessionState.getUserFromAuthenticator();

    GrantDesc grantDesc = new GrantDesc(privilegeObj, privilegeDesc,
        principalDesc, userName, PrincipalType.USER, grantOption);
    return TaskFactory.get(new DDLWork(inputs, outputs, grantDesc));
  }

  @Override
  public Task<? extends Serializable> createRevokeTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {
    List<PrivilegeDesc> privilegeDesc = analyzePrivilegeListDef((ASTNode) ast.getChild(0));
    List<PrincipalDesc> principalDesc = AuthorizationParseUtils.analyzePrincipalListDef((ASTNode) ast.getChild(1));
    PrivilegeObjectDesc hiveObj = null;
    boolean grantOption = false;
    if (ast.getChildCount() > 2) {
      ASTNode astChild = (ASTNode) ast.getChild(2);
      hiveObj = analyzePrivilegeObject(astChild, outputs);

      if (null != ast.getFirstChildWithType(HiveParser.TOK_GRANT_OPTION_FOR)) {
        grantOption = true;
      }
    }
    RevokeDesc revokeDesc = new RevokeDesc(privilegeDesc, principalDesc, hiveObj, grantOption);
    return TaskFactory.get(new DDLWork(inputs, outputs, revokeDesc));
  }
  @Override
  public Task<? extends Serializable> createShowGrantTask(ASTNode ast, Path resultFile, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {

    PrincipalDesc principalDesc = null;
    PrivilegeObjectDesc privHiveObj = null;

    ASTNode param = null;
    if (ast.getChildCount() > 0) {
      param = (ASTNode) ast.getChild(0);
      principalDesc = AuthorizationParseUtils.getPrincipalDesc(param);
      if (principalDesc != null) {
        param = (ASTNode) ast.getChild(1);  // shift one
      }
    }

    if (param != null) {
      if (param.getType() == HiveParser.TOK_RESOURCE_ALL) {
        privHiveObj = new PrivilegeObjectDesc(true, null, null, null);
      } else if (param.getType() == HiveParser.TOK_PRIV_OBJECT_COL) {
        privHiveObj = parsePrivObject(param);
      }
    }

    ShowGrantDesc showGrant = new ShowGrantDesc(resultFile.toString(), principalDesc, privHiveObj);
    return TaskFactory.get(new DDLWork(inputs, outputs, showGrant));
  }
  @Override
  public Task<? extends Serializable> createGrantRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) {
    return analyzeGrantRevokeRole(true, ast, inputs, outputs);
  }
  @Override
  public Task<? extends Serializable> createRevokeRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) {
    return analyzeGrantRevokeRole(false, ast, inputs, outputs);
  }
  private Task<? extends Serializable> analyzeGrantRevokeRole(boolean isGrant, ASTNode ast,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) {
    List<PrincipalDesc> principalDesc = AuthorizationParseUtils.analyzePrincipalListDef(
        (ASTNode) ast.getChild(0));

    //check if admin option has been specified
    int rolesStartPos = 1;
    ASTNode wAdminOption = (ASTNode) ast.getChild(1);
    boolean isAdmin = false;
    if((isGrant && wAdminOption.getToken().getType() == HiveParser.TOK_GRANT_WITH_ADMIN_OPTION) ||
       (!isGrant && wAdminOption.getToken().getType() == HiveParser.TOK_ADMIN_OPTION_FOR)){
      rolesStartPos = 2; //start reading role names from next position
      isAdmin = true;
    }

    List<String> roles = new ArrayList<String>();
    for (int i = rolesStartPos; i < ast.getChildCount(); i++) {
      roles.add(BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(i).getText()));
    }

    String roleOwnerName = SessionState.getUserFromAuthenticator();

    //until change is made to use the admin option. Default to false with V2 authorization

    if (isGrant) {
      GrantRoleDesc grantRoleDesc = new GrantRoleDesc(roles, principalDesc, roleOwnerName, isAdmin);
      return TaskFactory.get(new DDLWork(inputs, outputs, grantRoleDesc));
    } else {
      RevokeRoleDesc revokeRoleDesc = new RevokeRoleDesc(roles, principalDesc, roleOwnerName, isAdmin);
      return TaskFactory.get(new DDLWork(inputs, outputs, revokeRoleDesc));
    }
  }

  private PrivilegeObjectDesc analyzePrivilegeObject(ASTNode ast,
      HashSet<WriteEntity> outputs)
      throws SemanticException {

    PrivilegeObjectDesc subject = parsePrivObject(ast);

    if (subject.getTable()) {
      Table tbl = getTable(subject.getObject());
      if (subject.getPartSpec() != null) {
        Partition part = getPartition(tbl, subject.getPartSpec());
        outputs.add(new WriteEntity(part, WriteEntity.WriteType.DDL_NO_LOCK));
      } else {
        outputs.add(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));
      }
    }

    return subject;
  }

  protected PrivilegeObjectDesc parsePrivObject(ASTNode ast) throws SemanticException {
    boolean isTable;
    String object = null;
    Map<String, String> partSpec = null;
    List<String> columns = null;

    ASTNode child = (ASTNode) ast.getChild(0);
    ASTNode gchild = (ASTNode)child.getChild(0);
    if (child.getType() == HiveParser.TOK_TABLE_TYPE) {
      isTable = true;
      String[] qualified = BaseSemanticAnalyzer.getQualifiedTableName(gchild);
      object = BaseSemanticAnalyzer.getDotName(qualified);
    } else if (child.getType() == HiveParser.TOK_URI_TYPE || child.getType() == HiveParser.TOK_SERVER_TYPE) {
      throw new SemanticException("Hive authorization does not support the URI or SERVER objects");
    } else {
      isTable = false;
      object = BaseSemanticAnalyzer.unescapeIdentifier(gchild.getText());
    }
    //if partition spec node is present, set partition spec
    for (int i = 1; i < child.getChildCount(); i++) {
      gchild = (ASTNode) child.getChild(i);
      if (gchild.getType() == HiveParser.TOK_PARTSPEC) {
        partSpec = DDLSemanticAnalyzer.getPartSpec(gchild);
      } else if (gchild.getType() == HiveParser.TOK_TABCOLNAME) {
        columns = BaseSemanticAnalyzer.getColumnNames(gchild);
      }
    }
    return new PrivilegeObjectDesc(isTable, object, partSpec, columns);
  }

  private List<PrivilegeDesc> analyzePrivilegeListDef(ASTNode node)
      throws SemanticException {
    List<PrivilegeDesc> ret = new ArrayList<PrivilegeDesc>();
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode privilegeDef = (ASTNode) node.getChild(i);
      ASTNode privilegeType = (ASTNode) privilegeDef.getChild(0);
      Privilege privObj = PrivilegeRegistry.getPrivilege(privilegeType.getType());

      if (privObj == null) {
        throw new SemanticException("Undefined privilege " + PrivilegeType.
            getPrivTypeByToken(privilegeType.getType()));
      }
      List<String> cols = null;
      if (privilegeDef.getChildCount() > 1) {
        cols = BaseSemanticAnalyzer.getColumnNames((ASTNode) privilegeDef.getChild(1));
      }
      PrivilegeDesc privilegeDesc = new PrivilegeDesc(privObj, cols);
      ret.add(privilegeDesc);
    }
    return ret;
  }

  private Table getTable(String tblName) throws SemanticException {
    return getTable(null, tblName);
  }

  private Table getTable(String database, String tblName)
      throws SemanticException {
    try {
      Table tab = database == null ? db.getTable(tblName, false)
          : db.getTable(database, tblName, false);
      if (tab == null) {
        throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName));
      }
      return tab;
    } catch (HiveException e) {
      if(e instanceof SemanticException) {
        throw (SemanticException)e;
      }
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tblName), e);
    }
  }

  private Partition getPartition(Table table, Map<String, String> partSpec)
      throws SemanticException {
    try {
      Partition partition = db.getPartition(table, partSpec, false);
      if (partition == null) {
        throw new SemanticException(toMessage(ErrorMsg.INVALID_PARTITION, partSpec));
      }
      return partition;
    } catch (HiveException e) {
      if(e instanceof SemanticException) {
        throw (SemanticException)e;
      }
      throw new SemanticException(toMessage(ErrorMsg.INVALID_PARTITION, partSpec), e);
    }

  }
  private String toMessage(ErrorMsg message, Object detail) {
    return detail == null ? message.getMsg() : message.getMsg(detail.toString());
  }

  @Override
  public Task<? extends Serializable> createSetRoleTask(String roleName,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs)
      throws SemanticException {
    SetRoleDesc setRoleDesc = new SetRoleDesc(roleName);
    return TaskFactory.get(new DDLWork(inputs, outputs, setRoleDesc));
  }

  @Override
  public Task<? extends Serializable> createShowCurrentRoleTask(
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs, Path resFile)
      throws SemanticException {
    ShowCurrentRoleDesc showCurrentRoleDesc = new ShowCurrentRoleDesc(resFile.toString());
    return TaskFactory.get(new DDLWork(inputs, outputs, showCurrentRoleDesc));
  }

  @Override
  public Task<? extends Serializable> createShowRolePrincipalsTask(ASTNode ast, Path resFile,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) throws SemanticException {
    String roleName;

    if (ast.getChildCount() == 1) {
      roleName = ast.getChild(0).getText();
    } else {
      // the parser should not allow this
      throw new AssertionError("Unexpected Tokens in SHOW ROLE PRINCIPALS");
    }

    ShowPrincipalsDesc showPrincipalsDesc = new ShowPrincipalsDesc(roleName, resFile.toString());
    return TaskFactory.get(new DDLWork(inputs, outputs, showPrincipalsDesc));
  }

  @Override
  public Task<? extends Serializable> createShowRolesTask(ASTNode ast, Path resFile,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) throws SemanticException {
    ShowRolesDesc showRolesDesc = new ShowRolesDesc(resFile.toString());
    return TaskFactory.get(new DDLWork(inputs, outputs, showRolesDesc));
  }

}
