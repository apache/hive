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

package org.apache.hadoop.hive.ql.ddl.privilege;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * Represents a database privilege.
 */
@Explain(displayName = "Privilege", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class PrivilegeDesc implements Serializable, Cloneable {
  private static final long serialVersionUID = 1L;

  private final Privilege privilege;
  private final List<String> columns;

  public PrivilegeDesc(Privilege privilege, List<String> columns) {
    super();
    this.privilege = privilege;
    this.columns = columns;
  }

  @Explain(displayName = "privilege", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Privilege getPrivilege() {
    return privilege;
  }

  @Explain(displayName = "columns", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getColumns() {
    return columns;
  }
}
