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

package org.apache.hadoop.hive.ql.ddl.table.info.show.properties;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process showing the table status.
 */
public class ShowTablePropertiesOperation extends DDLOperation<ShowTablePropertiesDesc> {
  public ShowTablePropertiesOperation(DDLOperationContext context, ShowTablePropertiesDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    String tableName = desc.getTableName();

    // show table properties - populate the output stream
    Table tbl = context.getDb().getTable(tableName, false);
    try {
      if (tbl == null) {
        String errMsg = "Table " + tableName + " does not exist";
        ShowUtils.writeToFile(errMsg, desc.getResFile(), context);
        return 0;
      }

      LOG.info("DDLTask: show properties for {}", tableName);

      StringBuilder builder = new StringBuilder();
      String propertyName = desc.getPropertyName();
      if (propertyName != null) {
        String propertyValue = tbl.getProperty(propertyName);
        if (propertyValue == null) {
          String errMsg = "Table " + tableName + " does not have property: " + propertyName;
          builder.append(errMsg);
        } else {
          ShowUtils.appendNonNull(builder, propertyName, true);
          ShowUtils.appendNonNull(builder, propertyValue);
        }
      } else {
        Map<String, String> properties = new TreeMap<String, String>(tbl.getParameters());
        for (Entry<String, String> entry : properties.entrySet()) {
          ShowUtils.appendNonNull(builder, entry.getKey(), true);
          ShowUtils.appendNonNull(builder, entry.getValue());
        }
      }

      LOG.info("DDLTask: written data for showing properties of {}", tableName);
      ShowUtils.writeToFile(builder.toString(), desc.getResFile(), context);
    } catch (IOException e) {
      LOG.info("show table properties: ", e);
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    }

    return 0;
  }
}
