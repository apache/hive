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

package org.apache.hive.hcatalog;

import org.apache.derby.security.SystemPermission;

import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Policy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * A security policy that grants usederbyinternals
 *
 * <p>
 *   HCatalog tests use Security Manager to handle exits.  With Derby version 10.14.1, if a
 *   security manager is configured, embedded Derby requires usederbyinternals permission, and
 *   that is checked directly using AccessController.checkPermission.  This class will be used to
 *   setup a security policy to grant usederbyinternals, in tests that use NoExitSecurityManager.
 * </p>
 */
public class DerbyPolicy extends Policy {

  private static PermissionCollection perms;

  public DerbyPolicy() {
    super();
    if (perms == null) {
      perms = new DerbyPermissionCollection();
      addPermissions();
    }
  }

  @Override
  public PermissionCollection getPermissions(CodeSource codesource) {
    return perms;
  }

  private void addPermissions() {
    SystemPermission systemPermission = new SystemPermission("engine", "usederbyinternals");
    perms.add(systemPermission);
  }

  class DerbyPermissionCollection extends PermissionCollection {

    ArrayList<Permission> perms = new ArrayList<Permission>();

    public void add(Permission p) {
      perms.add(p);
    }

    public boolean implies(Permission p) {
      for (Iterator<Permission> i = perms.iterator(); i.hasNext();) {
        if (((Permission) i.next()).implies(p)) {
          return true;
        }
      }
      return false;
    }

    public Enumeration<Permission> elements() {
      return Collections.enumeration(perms);
    }

    public boolean isReadOnly() {
      return false;
    }
  }
}

