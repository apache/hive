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
import org.apache.hadoop.hive.ql.parse.HiveParser;

/**
 * Privilege defines a privilege in Hive. Each privilege has a name and scope associated with it.
 * This class contains all of the predefined privileges in Hive.
 */
public class Privilege {
  
  public enum PrivilegeType {
    ALL,
    ALTER_DATA,
    ALTER_METADATA,
    CREATE,
    DROP,
    INDEX,
    LOCK,
    SELECT,
    SHOW_DATABASE,
    UNKNOWN
  }


  public static PrivilegeType getPrivTypeByToken(int token) {
    switch (token) {
    case HiveParser.TOK_PRIV_ALL:
      return PrivilegeType.ALL;
    case HiveParser.TOK_PRIV_ALTER_DATA:
      return PrivilegeType.ALTER_DATA;
    case HiveParser.TOK_PRIV_ALTER_METADATA:
      return PrivilegeType.ALTER_METADATA;
    case HiveParser.TOK_PRIV_CREATE:
      return PrivilegeType.CREATE;
    case HiveParser.TOK_PRIV_DROP:
      return PrivilegeType.DROP;
    case HiveParser.TOK_PRIV_INDEX:
      return PrivilegeType.INDEX;
    case HiveParser.TOK_PRIV_LOCK:
      return PrivilegeType.LOCK;
    case HiveParser.TOK_PRIV_SELECT:
      return PrivilegeType.SELECT;
    case HiveParser.TOK_PRIV_SHOW_DATABASE:
      return PrivilegeType.SHOW_DATABASE;
    default:
      return PrivilegeType.UNKNOWN;
    }
  }

  public static PrivilegeType getPrivTypeByName(String privilegeName) {
    String canonicalizedName = privilegeName.toLowerCase();
    if (canonicalizedName.equals("all")) {
      return PrivilegeType.ALL;
    } else if (canonicalizedName.equals("update")) {
      return PrivilegeType.ALTER_DATA;
    } else if (canonicalizedName.equals("alter")) {
      return PrivilegeType.ALTER_METADATA;
    } else if (canonicalizedName.equals("create")) {
      return PrivilegeType.CREATE;
    } else if (canonicalizedName.equals("drop")) {
      return PrivilegeType.DROP;
    } else if (canonicalizedName.equals("index")) {
      return PrivilegeType.INDEX;
    } else if (canonicalizedName.equals("lock")) {
      return PrivilegeType.LOCK;
    } else if (canonicalizedName.equals("select")) {
      return PrivilegeType.SELECT;
    } else if (canonicalizedName.equals("show_database")) {
      return PrivilegeType.SHOW_DATABASE;
    }

    return PrivilegeType.UNKNOWN;
  }

  private PrivilegeType priv;
  
  private EnumSet<PrivilegeScope> supportedScopeSet;
  
  private Privilege(PrivilegeType priv, EnumSet<PrivilegeScope> scopeSet) {
    super();
    this.priv = priv;
    this.supportedScopeSet = scopeSet;
  }

  public Privilege(PrivilegeType priv) {
    super();
    this.priv = priv;
    
  }

  public PrivilegeType getPriv() {
    return priv;
  }

  public void setPriv(PrivilegeType priv) {
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
  
  @Override
  public String toString() {
    switch (this.priv) {
    case ALL:
      return "All";
    case ALTER_DATA:
      return "Update";
    case ALTER_METADATA:
      return "Alter";
    case CREATE:
      return "Create";
    case DROP:
      return "Drop";
    case INDEX:
      return "Index";
    case LOCK:
      return "Lock";
    case SELECT:
      return "Select";
    case SHOW_DATABASE:
      return "Show_Database";
    default:
      return "Unknown";
    }
  }

  public Privilege() {
  }

  public static Privilege ALL = new Privilege(PrivilegeType.ALL,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege ALTER_METADATA = new Privilege(PrivilegeType.ALTER_METADATA,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege ALTER_DATA = new Privilege(PrivilegeType.ALTER_DATA,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege CREATE = new Privilege(PrivilegeType.CREATE,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege DROP = new Privilege(PrivilegeType.DROP,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege INDEX = new Privilege(PrivilegeType.INDEX,
      PrivilegeScope.ALLSCOPE);

  public static Privilege LOCK = new Privilege(PrivilegeType.LOCK,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege SELECT = new Privilege(PrivilegeType.SELECT,
      PrivilegeScope.ALLSCOPE);

  public static Privilege SHOW_DATABASE = new Privilege(PrivilegeType.SHOW_DATABASE,
      EnumSet.of(PrivilegeScope.USER_LEVEL_SCOPE));

}
