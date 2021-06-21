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

package org.apache.hadoop.hive.ql.ddl.dataconnector.desc;

import java.io.DataOutputStream;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of describing a data connector.
 */
public class DescDataConnectorOperation extends DDLOperation<DescDataConnectorDesc> {
  public DescDataConnectorOperation(DDLOperationContext context, DescDataConnectorDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    try (DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      DataConnector connector = context.getDb().getDataConnector(desc.getConnectorName());
      if (connector == null) {
        throw new HiveException(ErrorMsg.DATACONNECTOR_NOT_EXISTS, desc.getConnectorName());
      }

      SortedMap<String, String> params = null;
      if (desc.isExtended()) {
        params = new TreeMap<>(connector.getParameters());
      }

      DescDataConnectorFormatter formatter = DescDataConnectorFormatter.getFormatter(context.getConf());
      formatter.showDataConnectorDescription(outStream, connector.getName(), connector.getType(),
          connector.getUrl(), connector.getOwnerName(), connector.getOwnerType(), connector.getDescription(), params);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }

    return 0;
  }
}
