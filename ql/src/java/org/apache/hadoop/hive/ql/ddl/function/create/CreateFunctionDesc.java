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

package org.apache.hadoop.hive.ql.ddl.function.create;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for CREATE [TEMPORARY] FUNCTION commands.
 */
@Explain(displayName = "Create Function", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateFunctionDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final String className;
  private final boolean isTemporary;
  private final List<ResourceUri> resources;
  private final ReplicationSpec replicationSpec;

  public CreateFunctionDesc(String name, String className, boolean isTemporary, List<ResourceUri> resources,
      ReplicationSpec replicationSpec) {
    this.name = name;
    this.className = className;
    this.isTemporary = isTemporary;
    this.resources = resources;
    this.replicationSpec = replicationSpec == null ? new ReplicationSpec() : replicationSpec;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getName() {
    return name;
  }

  @Explain(displayName = "class", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getClassName() {
    return className;
  }

  @Explain(displayName = "temporary", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isTemp() {
    return isTemporary;
  }

  public List<ResourceUri> getResources() {
    return resources;
  }

  /**
   * @return what kind of replication scope this create is running under.
   * This can result in a "CREATE IF NEWER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }
}
