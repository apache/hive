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
package org.apache.hadoop.hive.ql.parse.authorization;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.GrantDesc;
import org.apache.hadoop.hive.ql.plan.GrantRevokeRoleDDL;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.RevokeDesc;
import org.apache.hadoop.hive.ql.plan.RoleDDLDesc;
import org.apache.hadoop.hive.ql.plan.RoleDDLDesc.RoleOperation;
import org.apache.hadoop.hive.ql.plan.ShowGrantDesc;
import org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestHiveAuthorizationTaskFactory {

  private static final String SELECT = "SELECT";
  private static final String DB = "default";
  private static final String TABLE = "table1";
  private static final String GROUP = "group1";
  private static final String ROLE = "role1";
  private static final String USER = "user1";

  private ParseDriver parseDriver;
  private DDLSemanticAnalyzer analyzer;
  private HiveConf conf;
  private Context context;
  private String currentUser;
  private Hive db;
  private Table table;
  private Partition partition;

  @Before
  public void setup() throws Exception {
    conf = new HiveConf();
    db = Mockito.mock(Hive.class);
    table = new Table(DB, TABLE);
    partition = new Partition(table);
    context = new Context(conf);
    parseDriver = new ParseDriver();
    analyzer = new DDLSemanticAnalyzer(conf, db);
    SessionState.start(conf);
    Mockito.when(db.getTable(DB, TABLE, false)).thenReturn(table);
    Mockito.when(db.getPartition(table, new HashMap<String, String>(), false))
      .thenReturn(partition);
    HadoopDefaultAuthenticator auth = new HadoopDefaultAuthenticator();
    auth.setConf(conf);
    currentUser = auth.getUserName();

  }
  /**
   * CREATE ROLE ...
   */
  @Test
  public void testCreateRole() throws Exception {
    DDLWork work = analyze(parse("CREATE ROLE " + ROLE));
    RoleDDLDesc roleDesc = work.getRoleDDLDesc();
    Assert.assertNotNull("Role should not be null", roleDesc);
    Assert.assertEquals(RoleOperation.CREATE_ROLE, roleDesc.getOperation());
    Assert.assertFalse("Did not expect a group", roleDesc.getGroup());
    Assert.assertEquals(ROLE, roleDesc.getName());
  }
  /**
   * DROP ROLE ...
   */
  @Test
  public void testDropRole() throws Exception {
    DDLWork work = analyze(parse("DROp ROLE " + ROLE));
    RoleDDLDesc roleDesc = work.getRoleDDLDesc();
    Assert.assertNotNull("Role should not be null", roleDesc);
    Assert.assertEquals(RoleOperation.DROP_ROLE, roleDesc.getOperation());
    Assert.assertFalse("Did not expect a group", roleDesc.getGroup());
    Assert.assertEquals(ROLE, roleDesc.getName());
  }
  /**
   * GRANT ... ON TABLE ... TO USER ...
   */
  @Test
  public void testGrantUserTable() throws Exception {
    DDLWork work = analyze(parse("GRANT " + SELECT + " ON TABLE " + TABLE + " TO USER " + USER));
    GrantDesc grantDesc = work.getGrantDesc();
    Assert.assertNotNull("Grant should not be null", grantDesc);
    for(PrincipalDesc principal : inList(grantDesc.getPrincipals()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.USER, principal.getType());
      Assert.assertEquals(USER, principal.getName());
    }
    for(PrivilegeDesc privilege : inList(grantDesc.getPrivileges()).ofSize(1)) {
      Assert.assertEquals(Privilege.SELECT, privilege.getPrivilege());
    }
    Assert.assertTrue("Expected table", grantDesc.getPrivilegeSubjectDesc().getTable());
    Assert.assertEquals(TABLE, grantDesc.getPrivilegeSubjectDesc().getObject());
  }
  /**
   * GRANT ... ON TABLE ... TO ROLE ...
   */
  @Test
  public void testGrantRoleTable() throws Exception {
    DDLWork work = analyze(parse("GRANT " + SELECT + " ON TABLE " + TABLE + " TO ROLE " + ROLE));
    GrantDesc grantDesc = work.getGrantDesc();
    Assert.assertNotNull("Grant should not be null", grantDesc);
    for(PrincipalDesc principal : inList(grantDesc.getPrincipals()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.ROLE, principal.getType());
      Assert.assertEquals(ROLE, principal.getName());
    }
    for(PrivilegeDesc privilege : inList(grantDesc.getPrivileges()).ofSize(1)) {
      Assert.assertEquals(Privilege.SELECT, privilege.getPrivilege());
    }
    Assert.assertTrue("Expected table", grantDesc.getPrivilegeSubjectDesc().getTable());
    Assert.assertEquals(TABLE, grantDesc.getPrivilegeSubjectDesc().getObject());
  }
  /**
   * GRANT ... ON TABLE ... TO GROUP ...
   */
  @Test
  public void testGrantGroupTable() throws Exception {
    DDLWork work = analyze(parse("GRANT " + SELECT + " ON TABLE " + TABLE + " TO GROUP " + GROUP));
    GrantDesc grantDesc = work.getGrantDesc();
    Assert.assertNotNull("Grant should not be null", grantDesc);
    for(PrincipalDesc principal : inList(grantDesc.getPrincipals()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.GROUP, principal.getType());
      Assert.assertEquals(GROUP, principal.getName());
    }
    for(PrivilegeDesc privilege : inList(grantDesc.getPrivileges()).ofSize(1)) {
      Assert.assertEquals(Privilege.SELECT, privilege.getPrivilege());
    }
    Assert.assertTrue("Expected table", grantDesc.getPrivilegeSubjectDesc().getTable());
    Assert.assertEquals(TABLE, grantDesc.getPrivilegeSubjectDesc().getObject());
  }
  /**
   * REVOKE ... ON TABLE ... FROM USER ...
   */
  @Test
  public void testRevokeUserTable() throws Exception {
    DDLWork work = analyze(parse("REVOKE " + SELECT + " ON TABLE " + TABLE + " FROM USER " + USER));
    RevokeDesc grantDesc = work.getRevokeDesc();
    Assert.assertNotNull("Revoke should not be null", grantDesc);
    for(PrincipalDesc principal : inList(grantDesc.getPrincipals()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.USER, principal.getType());
      Assert.assertEquals(USER, principal.getName());
    }
    for(PrivilegeDesc privilege : inList(grantDesc.getPrivileges()).ofSize(1)) {
      Assert.assertEquals(Privilege.SELECT, privilege.getPrivilege());
    }
    Assert.assertTrue("Expected table", grantDesc.getPrivilegeSubjectDesc().getTable());
    Assert.assertEquals(TABLE, grantDesc.getPrivilegeSubjectDesc().getObject());
  }
  /**
   * REVOKE ... ON TABLE ... FROM ROLE ...
   */
  @Test
  public void testRevokeRoleTable() throws Exception {
    DDLWork work = analyze(parse("REVOKE " + SELECT + " ON TABLE " + TABLE + " FROM ROLE " + ROLE));
    RevokeDesc grantDesc = work.getRevokeDesc();
    Assert.assertNotNull("Revoke should not be null", grantDesc);
    for(PrincipalDesc principal : inList(grantDesc.getPrincipals()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.ROLE, principal.getType());
      Assert.assertEquals(ROLE, principal.getName());
    }
    for(PrivilegeDesc privilege : inList(grantDesc.getPrivileges()).ofSize(1)) {
      Assert.assertEquals(Privilege.SELECT, privilege.getPrivilege());
    }
    Assert.assertTrue("Expected table", grantDesc.getPrivilegeSubjectDesc().getTable());
    Assert.assertEquals(TABLE, grantDesc.getPrivilegeSubjectDesc().getObject());
  }
  /**
   * REVOKE ... ON TABLE ... FROM GROUP ...
   */
  @Test
  public void testRevokeGroupTable() throws Exception {
    DDLWork work = analyze(parse("REVOKE " + SELECT + " ON TABLE " + TABLE + " FROM GROUP " + GROUP));
    RevokeDesc grantDesc = work.getRevokeDesc();
    Assert.assertNotNull("Revoke should not be null", grantDesc);
    for(PrincipalDesc principal : inList(grantDesc.getPrincipals()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.GROUP, principal.getType());
      Assert.assertEquals(GROUP, principal.getName());
    }
    for(PrivilegeDesc privilege : inList(grantDesc.getPrivileges()).ofSize(1)) {
      Assert.assertEquals(Privilege.SELECT, privilege.getPrivilege());
    }
    Assert.assertTrue("Expected table", grantDesc.getPrivilegeSubjectDesc().getTable());
    Assert.assertEquals(TABLE, grantDesc.getPrivilegeSubjectDesc().getObject());
  }
  /**
   * GRANT ROLE ... TO USER ...
   */
  @Test
  public void testGrantRoleUser() throws Exception {
    DDLWork work = analyze(parse("GRANT ROLE " + ROLE + " TO USER " + USER));
    GrantRevokeRoleDDL grantDesc = work.getGrantRevokeRoleDDL();
    Assert.assertNotNull("Grant should not be null", grantDesc);
    Assert.assertTrue("Expected grant ", grantDesc.getGrant());
    Assert.assertTrue("Grant option is always true ", grantDesc.isGrantOption());
    Assert.assertEquals(currentUser, grantDesc.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantDesc.getGrantorType());
    for(String role : inList(grantDesc.getRoles()).ofSize(1)) {
      Assert.assertEquals(ROLE, role);
    }
    for(PrincipalDesc principal : inList(grantDesc.getPrincipalDesc()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.USER, principal.getType());
      Assert.assertEquals(USER, principal.getName());
    }
  }
  /**
   * GRANT ROLE ... TO ROLE ...
   */
  @Test
  public void testGrantRoleRole() throws Exception {
    DDLWork work = analyze(parse("GRANT ROLE " + ROLE + " TO ROLE " + ROLE));
    GrantRevokeRoleDDL grantDesc = work.getGrantRevokeRoleDDL();
    Assert.assertNotNull("Grant should not be null", grantDesc);
    Assert.assertTrue("Expected grant ", grantDesc.getGrant());
    Assert.assertTrue("Grant option is always true ", grantDesc.isGrantOption());
    Assert.assertEquals(currentUser, grantDesc.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantDesc.getGrantorType());
    for(String role : inList(grantDesc.getRoles()).ofSize(1)) {
      Assert.assertEquals(ROLE, role);
    }
    for(PrincipalDesc principal : inList(grantDesc.getPrincipalDesc()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.ROLE, principal.getType());
      Assert.assertEquals(ROLE, principal.getName());
    }
  }
  /**
   * GRANT ROLE ... TO GROUP ...
   */
  @Test
  public void testGrantRoleGroup() throws Exception {
    DDLWork work = analyze(parse("GRANT ROLE " + ROLE + " TO GROUP " + GROUP));
    GrantRevokeRoleDDL grantDesc = work.getGrantRevokeRoleDDL();
    Assert.assertNotNull("Grant should not be null", grantDesc);
    Assert.assertTrue("Expected grant ", grantDesc.getGrant());
    Assert.assertTrue("Grant option is always true ", grantDesc.isGrantOption());
    Assert.assertEquals(currentUser, grantDesc.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantDesc.getGrantorType());
    for(String role : inList(grantDesc.getRoles()).ofSize(1)) {
      Assert.assertEquals(ROLE, role);
    }
    for(PrincipalDesc principal : inList(grantDesc.getPrincipalDesc()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.GROUP, principal.getType());
      Assert.assertEquals(GROUP, principal.getName());
    }
  }
  /**
   * REVOKE ROLE ... FROM USER ...
   */
  @Test
  public void testRevokeRoleUser() throws Exception {
    DDLWork work = analyze(parse("REVOKE ROLE " + ROLE + " FROM USER " + USER));
    GrantRevokeRoleDDL grantDesc = work.getGrantRevokeRoleDDL();
    Assert.assertNotNull("Grant should not be null", grantDesc);
    Assert.assertFalse("Did not expect grant ", grantDesc.getGrant());
    Assert.assertTrue("Grant option is always true ", grantDesc.isGrantOption());
    Assert.assertEquals(currentUser, grantDesc.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantDesc.getGrantorType());
    for(String role : inList(grantDesc.getRoles()).ofSize(1)) {
      Assert.assertEquals(ROLE, role);
    }
    for(PrincipalDesc principal : inList(grantDesc.getPrincipalDesc()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.USER, principal.getType());
      Assert.assertEquals(USER, principal.getName());
    }
  }
  /**
   * REVOKE ROLE ... FROM ROLE ...
   */
  @Test
  public void testRevokeRoleRole() throws Exception {
    DDLWork work = analyze(parse("REVOKE ROLE " + ROLE + " FROM ROLE " + ROLE));
    GrantRevokeRoleDDL grantDesc = work.getGrantRevokeRoleDDL();
    Assert.assertNotNull("Grant should not be null", grantDesc);
    Assert.assertFalse("Did not expect grant ", grantDesc.getGrant());
    Assert.assertTrue("Grant option is always true ", grantDesc.isGrantOption());
    Assert.assertEquals(currentUser, grantDesc.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantDesc.getGrantorType());
    for(String role : inList(grantDesc.getRoles()).ofSize(1)) {
      Assert.assertEquals(ROLE, role);
    }
    for(PrincipalDesc principal : inList(grantDesc.getPrincipalDesc()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.ROLE, principal.getType());
      Assert.assertEquals(ROLE, principal.getName());
    }
  }
  /**
   * REVOKE ROLE ... FROM GROUP ...
   */
  @Test
  public void testRevokeRoleGroup() throws Exception {
    DDLWork work = analyze(parse("REVOKE ROLE " + ROLE + " FROM GROUP " + GROUP));
    GrantRevokeRoleDDL grantDesc = work.getGrantRevokeRoleDDL();
    Assert.assertNotNull("Grant should not be null", grantDesc);
    Assert.assertFalse("Did not expect grant ", grantDesc.getGrant());
    Assert.assertTrue("Grant option is always true ", grantDesc.isGrantOption());
    Assert.assertEquals(currentUser, grantDesc.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantDesc.getGrantorType());
    for(String role : inList(grantDesc.getRoles()).ofSize(1)) {
      Assert.assertEquals(ROLE, role);
    }
    for(PrincipalDesc principal : inList(grantDesc.getPrincipalDesc()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.GROUP, principal.getType());
      Assert.assertEquals(GROUP, principal.getName());
    }
  }
  /**
   * SHOW ROLE GRANT USER ...
   */
  @Test
  public void testShowRoleGrantUser() throws Exception {
    DDLWork work = analyze(parse("SHOW ROLE GRANT USER " + USER));
    RoleDDLDesc roleDesc = work.getRoleDDLDesc();
    Assert.assertNotNull("Role should not be null", roleDesc);
    Assert.assertEquals(RoleOperation.SHOW_ROLE_GRANT, roleDesc.getOperation());
    Assert.assertEquals(PrincipalType.USER, roleDesc.getPrincipalType());
    Assert.assertEquals(USER, roleDesc.getName());
  }
  /**
   * SHOW ROLE GRANT ROLE ...
   */
  @Test
  public void testShowRoleGrantRole() throws Exception {
    DDLWork work = analyze(parse("SHOW ROLE GRANT ROLE " + ROLE));
    RoleDDLDesc roleDesc = work.getRoleDDLDesc();
    Assert.assertNotNull("Role should not be null", roleDesc);
    Assert.assertEquals(RoleOperation.SHOW_ROLE_GRANT, roleDesc.getOperation());
    Assert.assertEquals(PrincipalType.ROLE, roleDesc.getPrincipalType());
    Assert.assertEquals(ROLE, roleDesc.getName());
  }
  /**
   * SHOW ROLE GRANT GROUP ...
   */
  @Test
  public void testShowRoleGrantGroup() throws Exception {
    DDLWork work = analyze(parse("SHOW ROLE GRANT GROUP " + GROUP));
    RoleDDLDesc roleDesc = work.getRoleDDLDesc();
    Assert.assertNotNull("Role should not be null", roleDesc);
    Assert.assertEquals(RoleOperation.SHOW_ROLE_GRANT, roleDesc.getOperation());
    Assert.assertEquals(PrincipalType.GROUP, roleDesc.getPrincipalType());
    Assert.assertEquals(GROUP, roleDesc.getName());
  }
  /**
   * SHOW GRANT USER ... ON TABLE ...
   */
  @Test
  public void testShowGrantUserOnTable() throws Exception {
    DDLWork work = analyze(parse("SHOW GRANT USER " + USER + " ON TABLE " + TABLE));
    ShowGrantDesc grantDesc = work.getShowGrantDesc();
    Assert.assertNotNull("Show grant should not be null", grantDesc);
    Assert.assertEquals(PrincipalType.USER, grantDesc.getPrincipalDesc().getType());
    Assert.assertEquals(USER, grantDesc.getPrincipalDesc().getName());
    Assert.assertTrue("Expected table", grantDesc.getHiveObj().getTable());
    Assert.assertEquals(TABLE, grantDesc.getHiveObj().getObject());
    Assert.assertTrue("Expected table", grantDesc.getHiveObj().getTable());
  }
  /**
   * SHOW GRANT ROLE ... ON TABLE ...
   */
  @Test
  public void testShowGrantRoleOnTable() throws Exception {
    DDLWork work = analyze(parse("SHOW GRANT ROLE " + ROLE + " ON TABLE " + TABLE));
    ShowGrantDesc grantDesc = work.getShowGrantDesc();
    Assert.assertNotNull("Show grant should not be null", grantDesc);
    Assert.assertEquals(PrincipalType.ROLE, grantDesc.getPrincipalDesc().getType());
    Assert.assertEquals(ROLE, grantDesc.getPrincipalDesc().getName());
    Assert.assertTrue("Expected table", grantDesc.getHiveObj().getTable());
    Assert.assertEquals(TABLE, grantDesc.getHiveObj().getObject());
    Assert.assertTrue("Expected table", grantDesc.getHiveObj().getTable());
  }
  /**
   * SHOW GRANT GROUP ... ON TABLE ...
   */
  @Test
  public void testShowGrantGroupOnTable() throws Exception {
    DDLWork work = analyze(parse("SHOW GRANT GROUP " + GROUP + " ON TABLE " + TABLE));
    ShowGrantDesc grantDesc = work.getShowGrantDesc();
    Assert.assertNotNull("Show grant should not be null", grantDesc);
    Assert.assertEquals(PrincipalType.GROUP, grantDesc.getPrincipalDesc().getType());
    Assert.assertEquals(GROUP, grantDesc.getPrincipalDesc().getName());
    Assert.assertTrue("Expected table", grantDesc.getHiveObj().getTable());
    Assert.assertEquals(TABLE, grantDesc.getHiveObj().getObject());
    Assert.assertTrue("Expected table", grantDesc.getHiveObj().getTable());
  }
  private ASTNode parse(String command) throws Exception {
    return ParseUtils.findRootNonNullToken(parseDriver.parse(command));
  }

  private DDLWork analyze(ASTNode ast) throws Exception {
    analyzer.analyze(ast, context);
    List<Task<? extends Serializable>> rootTasks = analyzer.getRootTasks();
    return (DDLWork) inList(rootTasks).ofSize(1).get(0).getWork();
  }
  private static class ListSizeMatcher<E> {
    private final List<E> list;
    private ListSizeMatcher(List<E> list) {
      this.list = list;
    }
    private List<E> ofSize(int size) {
      Assert.assertEquals(list.toString(),  size, list.size());
      return list;
    }
  }
  private static <E> ListSizeMatcher<E> inList(List<E> list) {
    return new ListSizeMatcher<E>(list);
  }
}
