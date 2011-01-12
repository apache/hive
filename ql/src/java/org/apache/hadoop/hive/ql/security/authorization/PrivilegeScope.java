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
 * PrivilegeScope describes a hive defined privilege's scope
 * (global/database/table/column). For example some hive privileges are
 * db-level only, some are global, and some are table only.
 */
public enum PrivilegeScope {
  
  USER_LEVEL_SCOPE((short) 0x01), 
  DB_LEVEL_SCOPE((short) 0x02), 
  TABLE_LEVEL_SCOPE((short) 0x04), 
  COLUMN_LEVEL_SCOPE((short) 0x08);

  private short mode;

  private PrivilegeScope(short mode) {
    this.mode = mode;
  }

  public short getMode() {
    return mode;
  }

  public void setMode(short mode) {
    this.mode = mode;
  }
  
  public static EnumSet<PrivilegeScope> ALLSCOPE = EnumSet.of(
      PrivilegeScope.USER_LEVEL_SCOPE, PrivilegeScope.DB_LEVEL_SCOPE,
      PrivilegeScope.TABLE_LEVEL_SCOPE, PrivilegeScope.COLUMN_LEVEL_SCOPE);

  public static EnumSet<PrivilegeScope> ALLSCOPE_EXCEPT_COLUMN = EnumSet.of(
      PrivilegeScope.USER_LEVEL_SCOPE, PrivilegeScope.DB_LEVEL_SCOPE,
      PrivilegeScope.TABLE_LEVEL_SCOPE);

}
