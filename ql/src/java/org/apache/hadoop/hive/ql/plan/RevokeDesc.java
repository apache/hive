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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;

@Explain(displayName="Revoke")
public class RevokeDesc extends DDLDesc implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  private List<PrivilegeDesc> privileges;

  private List<PrincipalDesc> principals;

  private PrivilegeObjectDesc privilegeSubjectDesc;
  
  public RevokeDesc(){
  }

  public RevokeDesc(List<PrivilegeDesc> privileges,
      List<PrincipalDesc> principals, PrivilegeObjectDesc privilegeSubjectDesc) {
    super();
    this.privileges = privileges;
    this.principals = principals;
    this.privilegeSubjectDesc = privilegeSubjectDesc;
  }

  public List<PrivilegeDesc> getPrivileges() {
    return privileges;
  }

  public void setPrivileges(List<PrivilegeDesc> privileges) {
    this.privileges = privileges;
  }

  public List<PrincipalDesc> getPrincipals() {
    return principals;
  }

  public void setPrincipals(List<PrincipalDesc> principals) {
    this.principals = principals;
  }

  public PrivilegeObjectDesc getPrivilegeSubjectDesc() {
    return privilegeSubjectDesc;
  }

  public void setPrivilegeSubjectDesc(PrivilegeObjectDesc privilegeSubjectDesc) {
    this.privilegeSubjectDesc = privilegeSubjectDesc;
  }
  
}
