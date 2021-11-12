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

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.dataconnector.alter.AbstractAlterDataConnectorOperation;

/**
 * Operation process of altering a dataconnector's properties.
 */
public class AlterDataConnectorSetPropertiesOperation
    extends AbstractAlterDataConnectorOperation<AlterDataConnectorSetPropertiesDesc> {
  public AlterDataConnectorSetPropertiesOperation(DDLOperationContext context, AlterDataConnectorSetPropertiesDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(DataConnector connector) {
    Map<String, String> newParams = desc.getConnectorProperties();
    Map<String, String> params = connector.getParameters();

    // if both old and new params are not null, merge them
    if (params != null && newParams != null) {
      params.putAll(newParams);
      connector.setParameters(params);
    } else {
      // if one of them is null, replace the old params with the new one
      connector.setParameters(newParams);
    }
  }
}
