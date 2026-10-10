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

package org.apache.hadoop.hive.ql.ddl.dataconnector.desc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.junit.Test;

/**
 * Tests for the DESC CONNECTOR formatters.
 *
 * <p>The DBCP connection properties carried in DCPROPERTIES are the same keys that
 * {@code PlanUtils.getPropertiesForExplain} drops from explain output (HIVE-28838).
 * The formatters here render the connector parameter map, so they are expected to keep
 * these values out of it too -- by withholding the value rather than dropping the key,
 * since a description is meant to report what is configured.</p>
 */
public class TestDescDataConnectorFormatter {

  private static final String CONNECTOR_NAME = "dc1";
  private static final String CONNECTOR_TYPE = "mysql";
  private static final String CONNECTOR_URL = "jdbc:mysql://example:3306/db1";
  private static final String OWNER = "owner1";
  private static final String COMMENT = "a connector";

  private static final String USERNAME_VALUE = "etl_acct_9x";
  private static final String PASSWORD_VALUE = "Zq7vNt2xKd";

  private static Map<String, String> connectionParams() {
    Map<String, String> params = new TreeMap<>();
    params.put(Constants.JDBC_USERNAME, USERNAME_VALUE);
    params.put(Constants.JDBC_PASSWORD, PASSWORD_VALUE);
    params.put(Constants.JDBC_DATABASE_TYPE, "MYSQL");
    return params;
  }

  private static String format(DescDataConnectorFormatter formatter) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      formatter.showDataConnectorDescription(out, CONNECTOR_NAME, CONNECTOR_TYPE, CONNECTOR_URL,
          OWNER, PrincipalType.USER, COMMENT, connectionParams());
    }
    return bytes.toString(StandardCharsets.UTF_8.name());
  }

  /**
   * The connection property keys stay in the listing -- so it is still apparent that they are
   * configured -- while their values are reported as withheld rather than rendered.
   */
  private static void assertValuesWithheld(String output) {
    // Unrelated parameters are still expected in the extended output.
    assertTrue("expected the non-connection parameters to be rendered",
        output.contains(Constants.JDBC_DATABASE_TYPE));

    assertTrue("expected the connection property keys to remain listed: " + output,
        output.contains(Constants.JDBC_USERNAME) && output.contains(Constants.JDBC_PASSWORD));
    assertTrue("expected the connection property values to read as withheld: " + output,
        output.contains(Constants.WITHHELD_VALUE));

    assertFalse("DBCP password value rendered in DESC CONNECTOR EXTENDED output: " + output,
        output.contains(PASSWORD_VALUE));
    assertFalse("DBCP username value rendered in DESC CONNECTOR EXTENDED output: " + output,
        output.contains(USERNAME_VALUE));
  }

  @Test
  public void testTextExtendedOutputOmitsConnectionPropertyValues() throws Exception {
    assertValuesWithheld(format(new DescDataConnectorFormatter.TextDescDataConnectorFormatter()));
  }

  @Test
  public void testJsonExtendedOutputOmitsConnectionPropertyValues() throws Exception {
    assertValuesWithheld(format(new DescDataConnectorFormatter.JsonDescDataConnectorFormatter()));
  }
}
