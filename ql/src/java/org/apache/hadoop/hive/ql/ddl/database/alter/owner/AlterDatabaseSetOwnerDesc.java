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

package org.apache.hadoop.hive.ql.ddl.database.alter.owner;

import org.apache.hadoop.hive.ql.ddl.database.alter.AbstractAlterDatabaseDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER DATABASE ... SET OWNER ... commands.
 */
@Explain(displayName = "Set Database Owner", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterDatabaseSetOwnerDesc extends AbstractAlterDatabaseDesc {
  private static final long serialVersionUID = 1L;

  private final PrincipalDesc ownerPrincipal;

  public AlterDatabaseSetOwnerDesc(String databaseName, PrincipalDesc ownerPrincipal, ReplicationSpec replicationSpec) {
    super(databaseName, replicationSpec);
    this.ownerPrincipal = ownerPrincipal;
  }

  @Explain(displayName="owner")
  public PrincipalDesc getOwnerPrincipal() {
    return ownerPrincipal;
  }
}
