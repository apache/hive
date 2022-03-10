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

package org.apache.hadoop.hive.ql.ddl.view.create;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for CREATE VIEW commands.
 */
@Explain(displayName = "Create View", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateViewDesc extends AbstractCreateViewDesc {
  private static final long serialVersionUID = 1L;

  private final String comment;
  private final Map<String, String> properties;
  private final List<String> partitionColumnNames;
  private final boolean ifNotExists;
  private final boolean replace;
  private final List<FieldSchema> partitionColumns;

  private ReplicationSpec replicationSpec = null;
  private String ownerName = null;

  public CreateViewDesc(String viewName, List<FieldSchema> schema, String comment, Map<String, String> properties,
      List<String> partitionColumnNames, boolean ifNotExists, boolean replace, String originalText,
      String expandedText, List<FieldSchema> partitionColumns) {
    super(viewName, schema, originalText, expandedText);
    this.comment = comment;
    this.properties = properties;
    this.partitionColumnNames = partitionColumnNames;
    this.ifNotExists = ifNotExists;
    this.replace = replace;
    this.partitionColumns = partitionColumns;
  }

  @Explain(displayName = "partition columns")
  public List<String> getPartColsString() {
    return Utilities.getFieldSchemaString(partitionColumns);
  }

  public List<FieldSchema> getPartitionColumns() {
    return partitionColumns;
  }

  public List<String> getPartColNames() {
    return partitionColumnNames;
  }

  @Explain(displayName = "comment")
  public String getComment() {
    return comment;
  }

  @Explain(displayName = "properties")
  public Map<String, String> getProperties() {
    return properties;
  }

  @Explain(displayName = "if not exists", displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  @Explain(displayName = "replace", displayOnlyOnTrue = true)
  public boolean isReplace() {
    return replace;
  }

  /**
   * @param replicationSpec Sets the replication spec governing this create.
   * This parameter will have meaningful values only for creates happening as a result of a replication.
   */
  public void setReplicationSpec(ReplicationSpec replicationSpec) {
    this.replicationSpec = replicationSpec;
  }

  /**
   * @return what kind of replication spec this create is running under.
   */
  public ReplicationSpec getReplicationSpec() {
    if (replicationSpec == null) {
      this.replicationSpec = new ReplicationSpec();
    }
    return replicationSpec;
  }

  public void setOwnerName(String ownerName) {
    this.ownerName = ownerName;
  }

  public String getOwnerName() {
    return ownerName;
  }
}
