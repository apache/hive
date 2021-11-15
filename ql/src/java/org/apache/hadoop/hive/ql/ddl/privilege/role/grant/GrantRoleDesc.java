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

package org.apache.hadoop.hive.ql.ddl.privilege.role.grant;

import java.util.List;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for GRANT ROLE commands.
 */
@Explain(displayName="Grant roles", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class GrantRoleDesc implements DDLDesc {

  private final List<String> roles;
  private final List<PrincipalDesc> principals;
  private final String grantor;
  private final boolean grantOption;

  public GrantRoleDesc(List<String> roles, List<PrincipalDesc> principals, String grantor, boolean grantOption) {
    this.principals = principals;
    this.roles = roles;
    this.grantor = grantor;
    this.grantOption = grantOption;
  }

  @Explain(displayName="principals", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<PrincipalDesc> getPrincipals() {
    return principals;
  }

  @Explain(displayName="roles", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getRoles() {
    return roles;
  }

  public String getGrantor() {
    return grantor;
  }

  public boolean isGrantOption() {
    return grantOption;
  }
}
