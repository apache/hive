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

package org.apache.hadoop.hive.ql.ddl.table.storage;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process of setting the location of a table.
 */
public class AlterTableSetLocationOperation extends AbstractAlterTableOperation<AlterTableSetLocationDesc> {
  public AlterTableSetLocationOperation(DDLOperationContext context, AlterTableSetLocationDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    StorageDescriptor sd = getStorageDescriptor(table, partition);
    String newLocation = desc.getLocation();
    try {
      URI locUri = new URI(newLocation);
      if (!new Path(locUri).isAbsolute()) {
        throw new HiveException(ErrorMsg.BAD_LOCATION_VALUE, newLocation);
      }
      sd.setLocation(newLocation);
    } catch (URISyntaxException e) {
      throw new HiveException(e);
    }
    environmentContext.getProperties().remove(StatsSetupConst.DO_NOT_UPDATE_STATS);
  }
}
