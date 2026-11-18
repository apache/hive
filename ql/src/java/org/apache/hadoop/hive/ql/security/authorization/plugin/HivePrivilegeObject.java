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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.metastore.HMSHandlerContext;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Represents the object on which privilege is being granted/revoked, and objects
 * being used in queries.
 *
 * Check the get* function documentation for information on what value it returns based on
 * the {@link HivePrivilegeObjectType}.
 *
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
public class HivePrivilegeObject implements Comparable<HivePrivilegeObject> {

  @Override
  public int compareTo(HivePrivilegeObject o) {
    int compare = type.compareTo(o.type);
    if (compare == 0) {
      compare = catName != null ?
          (o.catName != null ? catName.compareTo(o.catName) : 1) :
          (o.catName != null ? -1 : 0);
    }
    if (compare == 0) {
      compare = dbname != null ?
          (o.dbname != null ? dbname.compareTo(o.dbname) : 1) :
          (o.dbname != null ? -1 : 0);
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
    if (compare == 0) {
      compare = className != null ?
          (o.className != null ? className.compareTo(o.className) : 1) :
          (o.className != null ? -1 : 0);
    }
    if (compare == 0) {
      compare = ownerName != null?
          (o.ownerName != null ? ownerName.compareTo(o.ownerName) : 1) :
          (o.ownerName != null ? -1 : 0);
    }
    if (compare == 0) {
      compare = ownerType != null?
          (o.ownerType != null ? ownerType.compareTo(o.ownerType) : 1) :
          (o.ownerType != null ? -1 : 0);
    }

    return compare;
  }

  private int compare(Collection<String> o1, Collection<String> o2) {
    Iterator<String> it1 = o1.iterator();
    Iterator<String> it2 = o2.iterator();
    while (it1.hasNext()) {
      if (!it2.hasNext()) {
        break;
      }
      String s1 = it1.next();
      String s2 = it2.next();
      int compare = s1 != null ?
          (s2 != null ? s1.compareTo(s2) : 1) :
            (s2 != null ? -1 : 0);
      if (compare != 0) {
        return compare;
      }
    }
    return o1.size() > o2.size() ? 1 : (o1.size() < o2.size() ? -1 : 0);
  }

  /**
   * @return the given catalog name if not blank, otherwise the current catalog of the session in HS2,
   * or the default catalog from the HMSHandler configuration (or a new metastore configuration).
   */
  private String getCatalogOrDefault(String catName) {
    if (StringUtils.isNotBlank(catName)) {
      return catName;
    } else if (SessionState.get() != null) {
      // HS2
      return SessionState.get().getCurrentCatalog();
    }
    // HMS or others
    Configuration configuration =
        HMSHandlerContext.getConfiguration().orElseGet(MetastoreConf::newMetastoreConf);
    return MetaStoreUtils.getDefaultCatalog(configuration);
  }

  /**
   * Note that GLOBAL, PARTITION, COLUMN fields are populated only for Hive's old default
   * authorization mode.
   * When the authorization manager is an instance of HiveAuthorizerFactory, these types are not
   * used.
   */
  public enum HivePrivilegeObjectType {
    GLOBAL, DATABASE, TABLE_OR_VIEW, PARTITION, COLUMN, LOCAL_URI, DFS_URI, COMMAND_PARAMS, FUNCTION,
    DATACONNECTOR,
    // HIVE_SERVICE refers to a logical service name. For now hiveserver2 hostname will be
    // used to give service actions a name. This is used by kill query command so it can
    // be authorized specifically to a service if necessary.
    SERVICE_NAME,
    SCHEDULED_QUERY, STORAGEHANDLER_URI
  }

  /**
   * When {@link HiveOperationType} is QUERY, this action type is set so that it is possible
   * to determine if the action type on this object is an INSERT or INSERT_OVERWRITE
   */
  public enum HivePrivObjectActionType {
    OTHER, INSERT, INSERT_OVERWRITE, UPDATE, DELETE
  }

  private final HivePrivilegeObjectType type;
  private final String catName;
  private final String dbname;
  private final String objectName;
  private final List<String> commandParams;
  private final List<String> partKeys;
  private final List<String> columns;
  private final HivePrivObjectActionType actionType;
  private final String className;
  private final String ownerName;
  private final PrincipalType ownerType;
  // cellValueTransformers is corresponding to the columns.
  // Its size should be the same as columns.
  // For example, if a table has two columns, "key" and "value"
  // we may mask "value" as "reverse(value)". Then cellValueTransformers
  // should be "key" and "reverse(value)"
  private List<String> cellValueTransformers;
  // rowFilterExpression is applied to the whole table, i.e., dbname.objectName
  // For example, rowFilterExpression can be "key % 2 = 0 and key < 10" and it
  // is applied to the table.
  private String rowFilterExpression;

  /**
   * Used by external authorizers (e.g. Ranger, Sentry). Use {@link #builder(HivePrivilegeObjectType)} instead.
   */
  @Deprecated
  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName) {
    this(type, null, dbname, objectName, null, null, null, null, null, null, null);
  }

  /**
   * Used by external authorizers (e.g. Ranger, Sentry). Use {@link #builder(HivePrivilegeObjectType)} instead.
   */
  @Deprecated
  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName,
      HivePrivObjectActionType actionType) {
    this(type, null, dbname, objectName, null, null, actionType, null, null, null, null);
  }

  /**
   * Used by external authorizers (e.g. Ranger, Sentry). Use {@link #builder(HivePrivilegeObjectType)} instead.
   */
  @Deprecated
  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName,
      List<String> partKeys, String column) {
    this(type, null, dbname, objectName, partKeys, column == null ? null : Arrays.asList(column),
        null, null, null, null, null);
  }

  /**
   * Create HivePrivilegeObject of type {@link HivePrivilegeObjectType#COMMAND_PARAMS}.
   * @param cmdParams
   * @return
   */
  public static HivePrivilegeObject createHivePrivilegeObject(List<String> cmdParams) {
    return builder(HivePrivilegeObjectType.COMMAND_PARAMS).commandParams(cmdParams).build();
  }

  private HivePrivilegeObject(HivePrivilegeObjectType type, String catName, String dbname, String objectName,
      List<String> partKeys, List<String> columns, HivePrivObjectActionType actionType, List<String> commandParams,
      String className, String ownerName, PrincipalType ownerType) {
    this.type = type;
    this.catName = getCatalogOrDefault(catName);
    this.dbname = dbname;
    if (type == HivePrivilegeObjectType.DATABASE && objectName == null) {
      this.objectName = dbname;
    } else {
      this.objectName = objectName;
    }
    this.partKeys = partKeys;
    this.columns = columns;
    // Older constructors defaulted a missing action type to OTHER. Authorizers such as
    // Ranger switch on it, so a null from any constructor is stored as OTHER.
    this.actionType = actionType == null ? HivePrivObjectActionType.OTHER : actionType;
    this.commandParams = commandParams;
    this.className = className;
    this.ownerName = ownerName;
    this.ownerType = ownerType;
  }

  public static HivePrivilegeObject forScheduledQuery(String owner, String clusterNamespace, String scheduleName) {
    return builder(HivePrivilegeObjectType.SCHEDULED_QUERY)
        .dbName(clusterNamespace)
        .objectName(scheduleName)
        .ownerName(owner)
        .build();
  }

  public static Builder builder(HivePrivilegeObjectType type) {
    return new Builder(type);
  }

  /**
   * Builds a {@link HivePrivilegeObject}. A catalog name left unset resolves to the session's
   * current catalog in HS2, or the default catalog otherwise. An action type left unset is
   * {@link HivePrivObjectActionType#OTHER}.
   */
  public static final class Builder {
    private final HivePrivilegeObjectType type;
    private String catName;
    private String dbName;
    private String objectName;
    private List<String> partKeys;
    private List<String> columns;
    private HivePrivObjectActionType actionType = HivePrivObjectActionType.OTHER;
    private List<String> commandParams;
    private String className;
    private String ownerName;
    private PrincipalType ownerType;

    private Builder(HivePrivilegeObjectType type) {
      this.type = type;
    }

    public Builder catName(String catName) {
      this.catName = catName;
      return this;
    }

    public Builder dbName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public Builder objectName(String objectName) {
      this.objectName = objectName;
      return this;
    }

    public Builder partKeys(List<String> partKeys) {
      this.partKeys = partKeys;
      return this;
    }

    public Builder columns(List<String> columns) {
      this.columns = columns;
      return this;
    }

    public Builder actionType(HivePrivObjectActionType actionType) {
      this.actionType = actionType == null ? HivePrivObjectActionType.OTHER : actionType;
      return this;
    }

    public Builder commandParams(List<String> commandParams) {
      this.commandParams = commandParams;
      return this;
    }

    public Builder className(String className) {
      this.className = className;
      return this;
    }

    public Builder ownerName(String ownerName) {
      this.ownerName = ownerName;
      return this;
    }

    public Builder ownerType(PrincipalType ownerType) {
      this.ownerType = ownerType;
      return this;
    }

    public HivePrivilegeObject build() {
      return new HivePrivilegeObject(type, catName, dbName, objectName, partKeys, columns, actionType,
          commandParams, className, ownerName, ownerType);
    }
  }

  public HivePrivilegeObjectType getType() {
    return type;
  }

  /**
   * @return the catalog name
   */
  public String getCatName() {
    return catName;
  }

  /**
   * @return the db name if type is DATABASE, TABLE, or FUNCTION
   */
  public String getDbname() {
    return dbname;
  }

  /**
   * @return name of table/view/uri/function name
   */
  public String getObjectName() {
    return objectName;
  }

  /**
   * See javadoc of {@link HivePrivObjectActionType}.
   * Never null. A null passed to a constructor is stored as {@link HivePrivObjectActionType#OTHER}.
   * @return action type
   */
  public HivePrivObjectActionType getActionType() {
    return actionType;
  }

  public List<String> getCommandParams() {
    return commandParams;
  }

  /**
   * @return  partiton key information. Used only for old default authorization mode.
   */
  public List<String> getPartKeys() {
    return partKeys;
  }

  /**
   * Applicable columns in this object, when the type is {@link HivePrivilegeObjectType#TABLE_OR_VIEW}
   * In case of DML read operations, this is the set of columns being used.
   * Column information is not set for DDL operations and for tables being written into
   * @return list of applicable columns
   */
  public List<String> getColumns() {
    return columns;
  }

  /**
   * The class name when the type is {@link HivePrivilegeObjectType#FUNCTION}
   * @return the class name
   */
  public String getClassName() {
    return className;
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
      name = getDbObjectName();
      if (partKeys != null) {
        name += partKeys.toString();
      }
      break;
    case FUNCTION:
      name = getDbObjectName();
      break;
    case COLUMN:
    case LOCAL_URI:
    case DFS_URI:
    case STORAGEHANDLER_URI:
      name = objectName;
      break;
    case COMMAND_PARAMS:
      name = commandParams.toString();
      break;
    case SERVICE_NAME:
      name = objectName;
      break;
    case DATACONNECTOR:
      name = objectName;
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

  /**
   * @return ownerName of the object
   */
  public String getOwnerName() {
    return this.ownerName;
  }

  /**
   * @return principal type of the owner
   */
  public PrincipalType getOwnerType() {
    return this.ownerType;
  }

  private String getDbObjectName() {
    if (dbname == null) {
      return objectName;
    }
    String qualifiedDb = catName == null ? dbname : DatabaseName.getQualified(catName, dbname);
    return qualifiedDb + "." + objectName;
  }

  public List<String> getCellValueTransformers() {
    return cellValueTransformers;
  }

  public void setCellValueTransformers(List<String> cellValueTransformers) {
    this.cellValueTransformers = cellValueTransformers;
  }

  public String getRowFilterExpression() {
    return rowFilterExpression;
  }

  public void setRowFilterExpression(String rowFilterExpression) {
    this.rowFilterExpression = rowFilterExpression;
  }

}
