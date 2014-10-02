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
import java.util.Locale;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeScope;

/**
 * Represents the hive privilege being granted/revoked
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
public class HivePrivilege implements Comparable<HivePrivilege> {
  @Override
  public String toString() {
    return "Privilege [name=" + name + ", columns=" + columns + "]";
  }

  private final String name;
  private final List<String> columns;
  private final List<String> supportedScope;

  public HivePrivilege(String name, List<String> columns) {
    this(name, columns, null);
  }

  public HivePrivilege(String name, List<String> columns, List<String> supportedScope) {
    this.name = name.toUpperCase(Locale.US);
    this.columns = columns;
    this.supportedScope = supportedScope;
  }

  public String getName() {
    return name;
  }

  public List<String> getColumns() {
    return columns;
  }

  public List<String> getSupportedScope() {
    return supportedScope;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columns == null) ? 0 : columns.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    HivePrivilege other = (HivePrivilege) obj;
    if (columns == null) {
      if (other.columns != null)
        return false;
    } else if (!columns.equals(other.columns))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }


  public boolean supportsScope(PrivilegeScope scope) {
    return supportedScope != null && supportedScope.contains(scope.name());
  }

  @Override
  public int compareTo(HivePrivilege privilege) {
    int compare = columns != null ?
        (privilege.columns != null ? compare(columns, privilege.columns) : 1) :
        (privilege.columns != null ? -1 : 0);
    if (compare == 0) {
      compare = name.compareTo(privilege.name);
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
}
