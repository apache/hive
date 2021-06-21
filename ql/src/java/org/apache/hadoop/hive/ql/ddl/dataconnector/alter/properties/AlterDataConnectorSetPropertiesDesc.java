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

package org.apache.hadoop.hive.ql.ddl.dataconnector.alter.properties;

import java.util.Map;

import org.apache.hadoop.hive.ql.ddl.dataconnector.alter.AbstractAlterDataConnectorDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER CONNECTOR ... SET PROPERTIES ... commands.
 */
@Explain(displayName = "Set Connector Properties", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterDataConnectorSetPropertiesDesc extends AbstractAlterDataConnectorDesc {
  private static final long serialVersionUID = 1L;

  private final Map<String, String> dcProperties;

  public AlterDataConnectorSetPropertiesDesc(String connectorName, Map<String, String> dcProperties) {
    super(connectorName);
    this.dcProperties = dcProperties;
  }

  @Explain(displayName="properties")
  public Map<String, String> getConnectorProperties() {
    return dcProperties;
  }
}
