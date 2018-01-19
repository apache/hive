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

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


@Explain(displayName = "Grant", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class GrantDesc extends DDLDesc implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  private List<PrivilegeDesc> privileges;

  private List<PrincipalDesc> principals;

  private boolean grantOption;
  
  private String grantor;
  
  private PrincipalType grantorType;

  private PrivilegeObjectDesc privilegeSubjectDesc;

  public GrantDesc(PrivilegeObjectDesc privilegeSubject,
      List<PrivilegeDesc> privilegeDesc, List<PrincipalDesc> principalDesc,
      String grantor, PrincipalType grantorType, boolean grantOption) {
    super();
    this.privilegeSubjectDesc = privilegeSubject;
    this.privileges = privilegeDesc;
    this.principals = principalDesc;
    this.grantor = grantor;
    this.grantorType = grantorType;
    this.grantOption = grantOption;
  }

  /**
   * @return privileges
   */
  @Explain(displayName = "Privileges", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<PrivilegeDesc> getPrivileges() {
    return privileges;
  }

  /**
   * @param privileges
   */
  public void setPrivileges(List<PrivilegeDesc> privileges) {
    this.privileges = privileges;
  }

  /**
   * @return principals 
   */
  @Explain(displayName = "Principals", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<PrincipalDesc> getPrincipals() {
    return principals;
  }

  /**
   * @param principals
   */
  public void setPrincipals(List<PrincipalDesc> principals) {
    this.principals = principals;
  }

  /**
   * @return grant option
   */
  @Explain(displayName = "grant option", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isGrantOption() {
    return grantOption;
  }

  /**
   * @param grantOption
   */
  public void setGrantOption(boolean grantOption) {
    this.grantOption = grantOption;
  }

  /**
   * @return privilege subject
   */
  @Explain(displayName="privilege subject", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public PrivilegeObjectDesc getPrivilegeSubjectDesc() {
    return privilegeSubjectDesc;
  }

  /**
   * @param privilegeSubjectDesc
   */
  public void setPrivilegeSubjectDesc(PrivilegeObjectDesc privilegeSubjectDesc) {
    this.privilegeSubjectDesc = privilegeSubjectDesc;
  }

  public String getGrantor() {
    return grantor;
  }

  public void setGrantor(String grantor) {
    this.grantor = grantor;
  }

  public PrincipalType getGrantorType() {
    return grantorType;
  }

  public void setGrantorType(PrincipalType grantorType) {
    this.grantorType = grantorType;
  }

}
