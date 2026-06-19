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

package org.apache.hadoop.hive.ql.ddl.table.storage.skewed;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process of setting the skewed location.
 */
public class AlterTableSetSkewedLocationOperation extends AbstractAlterTableOperation<AlterTableSetSkewedLocationDesc> {
  public AlterTableSetSkewedLocationOperation(DDLOperationContext context, AlterTableSetSkewedLocationDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    // process location one-by-one
    for (Map.Entry<List<String>, String> entry : desc.getSkewedLocations().entrySet()) {
      List<String> key = entry.getKey();
      String newLocation = entry.getValue();
      try {
        URI locationUri = new URI(newLocation);
        List<String> skewedLocation = new ArrayList<String>(key);
        if (partition != null) {
          partition.setSkewedValueLocationMap(skewedLocation, locationUri.toString());
        } else {
          table.setSkewedValueLocationMap(skewedLocation, locationUri.toString());
        }
      } catch (URISyntaxException e) {
        throw new HiveException(e);
      }
    }

    environmentContext.getProperties().remove(StatsSetupConst.DO_NOT_UPDATE_STATS);
  }
}
