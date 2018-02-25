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
package org.apache.hive.hcatalog.templeton;

/**
 * The base create permissions for ddl objects.
 */
public abstract class GroupPermissionsDesc {
  public String group;
  public String permissions;

  public GroupPermissionsDesc() {}

  protected static boolean xequals(Object a, Object b) {
    if (a == null) {
      if (b == null)
        return true;
      else
        return false;
    }

    return a.equals(b);
  }

  protected static boolean xequals(boolean a, boolean b) { return a == b; }
  protected static boolean xequals(int a, int b)         { return a == b; }
  protected static boolean xequals(char a, char b)       { return a == b; }

  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (! (o instanceof GroupPermissionsDesc))
      return false;
    GroupPermissionsDesc that = (GroupPermissionsDesc) o;
    return xequals(this.group,       that.group)
      && xequals(this.permissions, that.permissions)
      ;
  }
}
