/**
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

package org.apache.hadoop.hive.ql.security.authorization.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

/**
 * Test HiveAuthorizer api invocation
 */
public class TestHiveAuthorizerCheckInvocation {
  protected static HiveConf conf;
  protected static Driver driver;
  private static final String tableName = TestHiveAuthorizerCheckInvocation.class.getSimpleName();
  static HiveAuthorizer mockedAuthorizer;

  /**
   * This factory creates a mocked HiveAuthorizer class. Use the mocked class to
   * capture the argument passed to it in the test case.
   */
  static class MockedHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator) {
      TestHiveAuthorizerCheckInvocation.mockedAuthorizer = Mockito.mock(HiveAuthorizer.class);
      return TestHiveAuthorizerCheckInvocation.mockedAuthorizer;
    }

  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    conf = new HiveConf();

    // Turn on mocked authorization
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, MockedHiveAuthorizerFactory.class.getName());
    conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

    SessionState.start(conf);
    driver = new Driver(conf);
    CommandProcessorResponse resp = driver.run("create table " + tableName
        + " (i int, j int, k string) partitioned by (city string, date string) ");
    assertEquals(0, resp.getResponseCode());
  }

  @AfterClass
  public static void afterTests() throws Exception {
    driver.close();
  }

  @Test
  public void testInputSomeColumnsUsed() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException {

    reset(mockedAuthorizer);
    int status = driver.compile("select i from " + tableName
        + " where k = 'X' and city = 'Scottsdale-AZ' ");
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs();
    checkSingleTableInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("no of columns used", 3, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("city", "i", "k"),
        getSortedList(tableObj.getColumns()));
  }

  private List<String> getSortedList(Set<String> columns) {
    List<String> sortedCols = new ArrayList<String>(columns);
    Collections.sort(sortedCols);
    return sortedCols;
  }

  @Test
  public void testInputAllColumnsUsed() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException {

    reset(mockedAuthorizer);
    int status = driver.compile("select * from " + tableName + " order by i");
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs();
    checkSingleTableInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("no of columns used", 5, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("city", "date", "i", "j", "k"),
        getSortedList(tableObj.getColumns()));
  }

  @Test
  public void testInputNoColumnsUsed() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException {

    reset(mockedAuthorizer);
    int status = driver.compile("describe " + tableName);
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs();
    checkSingleTableInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertNull("columns used", tableObj.getColumns());
  }

  private void checkSingleTableInput(List<HivePrivilegeObject> inputs) {
    assertEquals("number of inputs", 1, inputs.size());

    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, tableObj.getType());
    assertTrue("table name", tableName.equalsIgnoreCase(tableObj.getObjectName()));
  }

  /**
   * @return the inputs passed in current call to authorizer.checkPrivileges
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  private List<HivePrivilegeObject> getHivePrivilegeObjectInputs() throws HiveAuthzPluginException,
      HiveAccessControlException {
    // Create argument capturer
    // a class variable cast to this generic of generic class
    Class<List<HivePrivilegeObject>> class_listPrivObjects = (Class) List.class;
    ArgumentCaptor<List<HivePrivilegeObject>> inputsCapturer = ArgumentCaptor
        .forClass(class_listPrivObjects);

    verify(mockedAuthorizer).checkPrivileges(any(HiveOperationType.class),
        inputsCapturer.capture(), Matchers.anyListOf(HivePrivilegeObject.class),
        any(HiveAuthzContext.class));

    return inputsCapturer.getValue();
  }

}
