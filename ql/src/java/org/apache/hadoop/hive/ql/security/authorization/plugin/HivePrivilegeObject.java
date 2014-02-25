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

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;

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
    case PARTITION:
      break;
    }
    return "Object [type=" + type + ", name=" + name + "]";

  }

  public enum HivePrivilegeObjectType { DATABASE, TABLE_OR_VIEW, PARTITION, LOCAL_URI, DFS_URI};
  private final HivePrivilegeObjectType type;
  private final String dbname;
  private final String tableviewname;

  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String tableViewURI){
    this.type = type;
    this.dbname = dbname;
    this.tableviewname = tableViewURI;
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
}
