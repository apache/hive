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

package org.apache.hadoop.hive.ql.ddl.database.alter.location;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.database.alter.AbstractAlterDatabaseOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of altering a database's location.
 */
public class AlterDatabaseSetLocationOperation extends AbstractAlterDatabaseOperation<AlterDatabaseSetLocationDesc> {
  public AlterDatabaseSetLocationOperation(DDLOperationContext context, AlterDatabaseSetLocationDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Database database, Map<String, String> params) throws HiveException {
    try {
      String newLocation = desc.getLocation();
      URI locationURI = new URI(newLocation);
      if (!locationURI.isAbsolute() || StringUtils.isBlank(locationURI.getScheme())) {
        throw new HiveException(ErrorMsg.BAD_LOCATION_VALUE, newLocation);
      }

      if (newLocation.equals(database.getLocationUri())) {
        LOG.info("AlterDatabase skipped. No change in location.");
      } else {
        database.setLocationUri(newLocation);
      }
    } catch (URISyntaxException e) {
      throw new HiveException(e);
    }
  }
}
