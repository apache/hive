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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;

/**
 * Represents the object on which privilege is being granted/revoked
 */
@LimitedPrivate(value = { "" })
@Unstable
public class HivePrivilegeObject {

  @Override
  public String toString() {
    String name = null;
    switch (type) {
    case DATABASE:
      name = dbname;
      break;
    case TABLE_OR_VIEW:
      name = (dbname == null ? "" : dbname + ".") + tableviewname;
      break;
    case LOCAL_URI:
    case DFS_URI:
      name = tableviewname;
      break;
    case COMMAND_PARAMS:
      name = commandParams.toString();
      break;
    case PARTITION:
      break;
    }
    return "Object [type=" + type + ", name=" + name + "]";

  }

  public enum HivePrivilegeObjectType {
    DATABASE, TABLE_OR_VIEW, PARTITION, LOCAL_URI, DFS_URI, COMMAND_PARAMS
  };

  public enum HivePrivObjectActionType {
    OTHER, INSERT, INSERT_OVERWRITE
  };
  private final HivePrivilegeObjectType type;
  private final String dbname;
  private final String tableviewname;
  private final List<String> commandParams;
  private final HivePrivObjectActionType actionType;

  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String tableViewURI){
    this(type, dbname, tableViewURI, HivePrivObjectActionType.OTHER);
  }

  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String tableViewURI,
      HivePrivObjectActionType actionType) {
    this(type, dbname, tableViewURI, actionType, null);
  }

  /**
   * Create HivePrivilegeObject of type {@link HivePrivilegeObjectType.COMMAND_PARAMS}
   * @param cmdParams
   * @return
   */
  public static HivePrivilegeObject createHivePrivilegeObject(List<String> cmdParams) {
    return new HivePrivilegeObject(HivePrivilegeObjectType.COMMAND_PARAMS, null, null, null,
        cmdParams);
  }

  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String tableViewURI,
      HivePrivObjectActionType actionType, List<String> commandParams) {
    this.type = type;
    this.dbname = dbname;
    this.tableviewname = tableViewURI;
    this.actionType = actionType;
    this.commandParams = commandParams;
  }

  public HivePrivilegeObjectType getType() {
    return type;
  }

  public String getDbname() {
    return dbname;
  }

  public String getTableViewURI() {
    return tableviewname;
  }

  public HivePrivObjectActionType getActionType() {
    return actionType;
  }

  public List<String> getCommandParams() {
    return commandParams;
  }
}
