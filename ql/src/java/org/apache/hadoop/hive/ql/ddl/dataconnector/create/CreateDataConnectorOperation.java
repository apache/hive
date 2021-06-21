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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Operation process of creating a dataconnector.
 */
public class CreateDataConnectorOperation extends DDLOperation<CreateDataConnectorDesc> {

  public CreateDataConnectorOperation(DDLOperationContext context, CreateDataConnectorDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    try {
      URI connectorUri = new URI(desc.getURL());
      if (!connectorUri.isAbsolute() || StringUtils.isBlank(connectorUri.getScheme())) {
        throw new HiveException(ErrorMsg.INVALID_PATH, desc.getURL()); // TODO make a new error message for URL
      }

      DataConnector connector = new DataConnector(desc.getName(), desc.getType(), desc.getURL());
      if (desc.getComment() != null)
        connector.setDescription(desc.getComment());
      connector.setOwnerName(SessionState.getUserFromAuthenticator());
      connector.setOwnerType(PrincipalType.USER);
      if (desc.getConnectorProperties() != null)
        connector.setParameters(desc.getConnectorProperties());
      try {
        context.getDb().createDataConnector(connector, desc.getIfNotExists());
      } catch (AlreadyExistsException ex) {
        //it would be better if AlreadyExistsException had an errorCode field....
        throw new HiveException(ex, ErrorMsg.DATACONNECTOR_ALREADY_EXISTS, desc.getName());
      }

      return 0;
    } catch (URISyntaxException e) {
      throw new HiveException(e);
    }

  }
}
