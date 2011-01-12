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

package org.apache.hadoop.hive.ql.security.authorization;

import java.util.EnumSet;

/**
 * Privilege defines a privilege in Hive. Each privilege has a name and scope associated with it.
 * This class contains all of the predefined privileges in Hive.
 */
public class Privilege {
  
  private String priv;
  
  private EnumSet<PrivilegeScope> supportedScopeSet;
  
  private Privilege(String priv, EnumSet<PrivilegeScope> scopeSet) {
    super();
    this.priv = priv;
    this.supportedScopeSet = scopeSet;
  }

  public Privilege(String priv) {
    super();
    this.priv = priv;
    
  }

  public String getPriv() {
    return priv;
  }

  public void setPriv(String priv) {
    this.priv = priv;
  }
  
  public boolean supportColumnLevel() {
    return supportedScopeSet != null
        && supportedScopeSet.contains(PrivilegeScope.COLUMN_LEVEL_SCOPE);
  }

  public boolean supportDBLevel() {
    return supportedScopeSet != null
        && supportedScopeSet.contains(PrivilegeScope.DB_LEVEL_SCOPE);
  }

  public boolean supportTableLevel() {
    return supportedScopeSet != null
        && supportedScopeSet.contains(PrivilegeScope.TABLE_LEVEL_SCOPE);
  }
  
  public String toString() {
    return this.priv;
  }

  public Privilege() {
  }

  public static Privilege ALL = new Privilege("All",
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege ALTER_METADATA = new Privilege("Alter",
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege ALTER_DATA = new Privilege("Update",
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege CREATE = new Privilege("Create",
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege DROP = new Privilege("Drop",
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege INDEX = new Privilege("Index",
      PrivilegeScope.ALLSCOPE);

  public static Privilege LOCK = new Privilege("Lock",
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege SELECT = new Privilege("Select",
      PrivilegeScope.ALLSCOPE);

  public static Privilege SHOW_DATABASE = new Privilege("Show_Database",
      EnumSet.of(PrivilegeScope.USER_LEVEL_SCOPE));

}
