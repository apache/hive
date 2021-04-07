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

package org.apache.hadoop.hive.ql.ddl.dataconnector.create;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for CREATE DATACONNECTOR commands.
 */
@Explain(displayName = "Create DataConnector", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateDataConnectorDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String connectorName;
  private final String type;
  private final String url;
  private final String description;
  private final boolean ifNotExists;
  private final Map<String, String> dcProperties;

  public CreateDataConnectorDesc(String connectorName, String type, String url, boolean ifNotExists, String description,
      Map<String, String> dcProperties) {
    this.connectorName = connectorName;
    this.type = type;
    this.url = url;
    this.ifNotExists = ifNotExists;
    this.dcProperties = dcProperties;
    this.description = description;
  }

  @Explain(displayName="if not exists", displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public Map<String, String> getConnectorProperties() {
    return dcProperties;
  }

  @Explain(displayName="name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getName() {
    return connectorName;
  }

  @Explain(displayName="description")
  public String getComment() {
    return description;
  }

  @Explain(displayName="url")
  public String getURL() {
    return url;
  }

  @Explain(displayName="connector type")
  public String getType() {
    return type;
  }
}
