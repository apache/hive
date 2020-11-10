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

package org.apache.hadoop.hive.ql.ddl.dataconnector.drop;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for DROP CONNECTOR commands.
 */
@Explain(displayName = "Drop Connector", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DropDataConnectorDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String connectorName;
  private final boolean ifExists;
  private final ReplicationSpec replicationSpec;

  public DropDataConnectorDesc(String connectorName, boolean ifExists, ReplicationSpec replicationSpec) {
    this(connectorName, ifExists, false, replicationSpec);
  }

  public DropDataConnectorDesc(String connectorName, boolean ifExists, boolean cascade, ReplicationSpec replicationSpec) {
    this.connectorName = connectorName;
    this.ifExists = ifExists;
    this.replicationSpec = replicationSpec;
  }

  @Explain(displayName = "connector", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getConnectorName() {
    return connectorName;
  }

  @Explain(displayName = "if exists")
  public boolean getIfExists() {
    return ifExists;
  }

  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }
}
