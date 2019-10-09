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

package org.apache.hadoop.hive.ql.ddl.privilege.grant;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrivilegeDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for GRANT commands.
 */
@Explain(displayName = "Grant", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class GrantDesc implements DDLDesc, Serializable, Cloneable {
  private static final long serialVersionUID = 1L;

  private final PrivilegeObjectDesc privilegeSubject;
  private final List<PrivilegeDesc> privileges;
  private final List<PrincipalDesc> principals;
  private final String grantor;
  private final PrincipalType grantorType;
  private final boolean grantOption;

  public GrantDesc(PrivilegeObjectDesc privilegeSubject, List<PrivilegeDesc> privileges, List<PrincipalDesc> principals,
      String grantor, PrincipalType grantorType, boolean grantOption) {
    this.privilegeSubject = privilegeSubject;
    this.privileges = privileges;
    this.principals = principals;
    this.grantor = grantor;
    this.grantorType = grantorType;
    this.grantOption = grantOption;
  }

  @Explain(skipHeader = true, explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public PrivilegeObjectDesc getPrivilegeSubject() {
    return privilegeSubject;
  }

  @Explain(displayName = "Privileges", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<PrivilegeDesc> getPrivileges() {
    return privileges;
  }

  @Explain(displayName = "Principals", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<PrincipalDesc> getPrincipals() {
    return principals;
  }

  public String getGrantor() {
    return grantor;
  }

  public PrincipalType getGrantorType() {
    return grantorType;
  }

  @Explain(displayName = "grant option", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isGrantOption() {
    return grantOption;
  }
}
