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

package org.apache.hadoop.hive.ql.ddl.dataconnector.alter;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of altering a data connector.
 */
public abstract class AbstractAlterDataConnectorOperation<T extends AbstractAlterDataConnectorDesc> extends DDLOperation<T> {
  public AbstractAlterDataConnectorOperation(DDLOperationContext context, T desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    String dcName = desc.getConnectorName();
    DataConnector connector = context.getDb().getDataConnector(dcName);
    if (connector == null) {
      throw new HiveException(ErrorMsg.DATACONNECTOR_NOT_EXISTS, dcName);
    }

    Map<String, String> params = connector.getParameters();
    // this call is to set the values from the alter descriptor onto the connector object
    doAlteration(connector);

    // This is the HMS metadata operation to modify the object
    context.getDb().alterDataConnector(connector.getName(), connector);
    return 0;
  }

  protected abstract void doAlteration(DataConnector connector) throws HiveException;
}
