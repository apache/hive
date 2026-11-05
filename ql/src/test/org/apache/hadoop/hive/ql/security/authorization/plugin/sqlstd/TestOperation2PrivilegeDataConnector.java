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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.Operation2Privilege.IOType;
import org.junit.Test;

/**
 * Privilege requirements the SQL standard authorizer applies to the data connector
 * operations.
 *
 * <p>Connector definitions carry the DBCP connection properties for the remote source,
 * and CREATE / ALTER / DROP CONNECTOR are all admin-only. The operations that render a
 * connector definition are expected to carry a requirement of their own rather than
 * resolving to an empty privilege set.</p>
 */
public class TestOperation2PrivilegeDataConnector {

  private static final String CONNECTOR_NAME = "dc1";

  private static HivePrivilegeObject connectorObject() {
    return new HivePrivilegeObject(HivePrivilegeObjectType.DATACONNECTOR, CONNECTOR_NAME);
  }

  @Test
  public void testDescribeConnectorHasAnInputRequirement() {
    RequiredPrivileges required =
        Operation2Privilege.getRequiredPrivs(HiveOperationType.DESCDATACONNECTOR, connectorObject(), IOType.INPUT);

    assertTrue("DESCDATACONNECTOR resolves to an empty input privilege set",
        !required.getRequiredPrivilegeSet().isEmpty()
            || Operation2Privilege.isAdminPrivOperation(HiveOperationType.DESCDATACONNECTOR));
  }

  @Test
  public void testShowConnectorsHasAnInputRequirement() {
    RequiredPrivileges required =
        Operation2Privilege.getRequiredPrivs(HiveOperationType.SHOWDATACONNECTORS, connectorObject(), IOType.INPUT);

    assertTrue("SHOWDATACONNECTORS resolves to an empty input privilege set",
        !required.getRequiredPrivilegeSet().isEmpty()
            || Operation2Privilege.isAdminPrivOperation(HiveOperationType.SHOWDATACONNECTORS));
  }
}
