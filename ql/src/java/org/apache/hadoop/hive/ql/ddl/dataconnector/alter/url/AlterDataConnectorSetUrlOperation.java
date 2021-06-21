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

package org.apache.hadoop.hive.ql.ddl.dataconnector.alter.url;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.dataconnector.alter.AbstractAlterDataConnectorOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of altering a connector's URL.
 */
public class AlterDataConnectorSetUrlOperation extends
    AbstractAlterDataConnectorOperation<AlterDataConnectorSetUrlDesc> {
  public AlterDataConnectorSetUrlOperation(DDLOperationContext context, AlterDataConnectorSetUrlDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(DataConnector connector) throws HiveException {
    try {
      String newUrl = desc.getURL();

      if (StringUtils.isBlank(newUrl) || newUrl.equals(connector.getUrl())) {
        throw new HiveException("New URL for data connector cannot be blank or the same as the current one");
      }

      URI newURI = new URI(newUrl);
      if (!newURI.isAbsolute() || StringUtils.isBlank(newURI.getScheme())) {
        throw new HiveException(ErrorMsg.INVALID_PATH, newUrl); // TODO make a new error message for URL
      }
      connector.setUrl(newUrl);
      return;
    } catch (URISyntaxException e) {
      throw new HiveException(e);
    }
  }
}
