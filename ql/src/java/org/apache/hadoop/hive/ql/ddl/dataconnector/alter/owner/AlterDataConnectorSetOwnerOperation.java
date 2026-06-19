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

package org.apache.hadoop.hive.ql.ddl.dataconnector.alter.owner;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.dataconnector.alter.AbstractAlterDataConnectorOperation;

/**
 * Operation process of altering a connector's owner.
 */
public class AlterDataConnectorSetOwnerOperation extends
    AbstractAlterDataConnectorOperation<AlterDataConnectorSetOwnerDesc> {
  public AlterDataConnectorSetOwnerOperation(DDLOperationContext context, AlterDataConnectorSetOwnerDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(DataConnector connector) {
    connector.setOwnerName(desc.getOwnerPrincipal().getName());
    connector.setOwnerType(desc.getOwnerPrincipal().getType());
  }
}
