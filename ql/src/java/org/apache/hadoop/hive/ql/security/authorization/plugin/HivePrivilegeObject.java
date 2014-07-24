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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;

/**
 * Represents the object on which privilege is being granted/revoked
 */
@LimitedPrivate(value = { "" })
@Unstable
public class HivePrivilegeObject implements Comparable<HivePrivilegeObject> {

  @Override
  public int compareTo(HivePrivilegeObject o) {
    int compare = type.compareTo(o.type);
    if (compare == 0) {
      compare = dbname.compareTo(o.dbname);
    }
    if (compare == 0) {
      compare = objectName != null ?
          (o.objectName != null ? objectName.compareTo(o.objectName) : 1) :
          (o.objectName != null ? -1 : 0);
    }
    if (compare == 0) {
      compare = partKeys != null ?
          (o.partKeys != null ? compare(partKeys, o.partKeys) : 1) :
          (o.partKeys != null ? -1 : 0);
    }
    if (compare == 0) {
      compare = columns != null ?
          (o.columns != null ? compare(columns, o.columns) : 1) :
          (o.columns != null ? -1 : 0);
    }
    return compare;
  }

  private int compare(List<String> o1, List<String> o2) {
    for (int i = 0; i < Math.min(o1.size(), o2.size()); i++) {
      int compare = o1.get(i).compareTo(o2.get(i));
      if (compare != 0) {
        return compare;
      }
    }
    return o1.size() > o2.size() ? 1 : (o1.size() < o2.size() ? -1 : 0);
  }

  public enum HivePrivilegeObjectType {
    GLOBAL, DATABASE, TABLE_OR_VIEW, PARTITION, COLUMN, LOCAL_URI, DFS_URI, COMMAND_PARAMS, FUNCTION
  } ;
  public enum HivePrivObjectActionType {
    OTHER, INSERT, INSERT_OVERWRITE
  };

  private final HivePrivilegeObjectType type;
  private final String dbname;
  private final String objectName;
  private final List<String> commandParams;
  private final List<String> partKeys;
  private final List<String> columns;
  private final HivePrivObjectActionType actionType;

  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName) {
    this(type, dbname, objectName, HivePrivObjectActionType.OTHER);
  }

  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName
      , HivePrivObjectActionType actionType) {
    this(type, dbname, objectName, null, null, actionType, null);
  }

  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName,
      List<String> partKeys, String column) {
    this(type, dbname, objectName, partKeys,
        column == null ? null : new ArrayList<String>(Arrays.asList(column)),
        HivePrivObjectActionType.OTHER, null);

  }

  /**
   * Create HivePrivilegeObject of type {@link HivePrivilegeObjectType.COMMAND_PARAMS}
   * @param cmdParams
   * @return
   */
  public static HivePrivilegeObject createHivePrivilegeObject(List<String> cmdParams) {
    return new HivePrivilegeObject(HivePrivilegeObjectType.COMMAND_PARAMS, null, null, null, null,
        cmdParams);
  }

  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName,
    List<String> partKeys, List<String> columns, List<String> commandParams) {
    this(type, dbname, objectName, partKeys, columns, HivePrivObjectActionType.OTHER, commandParams);
  }

  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName,
      List<String> partKeys, List<String> columns, HivePrivObjectActionType actionType,
      List<String> commandParams) {
    this.type = type;
    this.dbname = dbname;
    this.objectName = objectName;
    this.partKeys = partKeys;
    this.columns = columns;
    this.actionType = actionType;
    this.commandParams = commandParams;
  }

  public HivePrivilegeObjectType getType() {
    return type;
  }

  public String getDbname() {
    return dbname;
  }

  /**
   * @return name of table/view/uri/function name
   */
  public String getObjectName() {
    return objectName;
  }

  public HivePrivObjectActionType getActionType() {
    return actionType;
  }

  public List<String> getCommandParams() {
    return commandParams;
  }

  public List<String> getPartKeys() {
    return partKeys;
  }

  public List<String> getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    String name = null;
    switch (type) {
    case DATABASE:
      name = dbname;
      break;
    case TABLE_OR_VIEW:
    case PARTITION:
      name = getDbObjectName(dbname, objectName);
      if (partKeys != null) {
        name += partKeys.toString();
      }
      break;
    case FUNCTION:
      name = getDbObjectName(dbname, objectName);
      break;
    case COLUMN:
    case LOCAL_URI:
    case DFS_URI:
      name = objectName;
      break;
    case COMMAND_PARAMS:
      name = commandParams.toString();
      break;
    }

    // get the string representing action type if its non default action type
    String actionTypeStr ="";
    if (actionType != null) {
      switch (actionType) {
      case INSERT:
      case INSERT_OVERWRITE:
        actionTypeStr = ", action=" + actionType;
      default:
      }
    }

    return "Object [type=" + type + ", name=" + name + actionTypeStr + "]";
  }

  private String getDbObjectName(String dbname2, String objectName2) {
    return (dbname == null ? "" : dbname + ".") + objectName;
  }

}
