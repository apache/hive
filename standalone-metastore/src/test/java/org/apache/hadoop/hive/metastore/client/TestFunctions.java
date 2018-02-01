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

package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.FunctionBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test class for IMetaStoreClient API. Testing the Function related functions.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestFunctions {
  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private static final String DEFAULT_DATABASE = "default";
  private static final String OTHER_DATABASE = "dummy";
  private static final String TEST_FUNCTION_CLASS =
      "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Function[] testFunctions = new Function[4];

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
        .map(test -> (AbstractMetaStoreService)test[1])
        .collect(Collectors.toSet());
    return result;
  }

  public TestFunctions(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
  }

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should move this to @AfterParam
  @AfterClass
  public static void stopMetaStores() throws Exception {
    for(AbstractMetaStoreService metaStoreService : metaStoreServices) {
      metaStoreService.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(OTHER_DATABASE, true, true, true);
    for(Function function : client.getAllFunctions().getFunctions()) {
      client.dropFunction(function.getDbName(), function.getFunctionName());
    }

    testFunctions[0] =
        new FunctionBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setName("test_function_to_find_1")
            .setClass(TEST_FUNCTION_CLASS)
            .addResourceUri(new ResourceUri(ResourceType.JAR, "hdfs:///tmp/jar1.jar"))
            .addResourceUri(new ResourceUri(ResourceType.FILE, "hdfs:///tmp/file1.txt"))
            .addResourceUri(new ResourceUri(ResourceType.ARCHIVE, "hdfs:///tmp/archive1.tgz"))
            .build();
    testFunctions[1] =
        new FunctionBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setName("test_function_to_find_2")
            .setClass(TEST_FUNCTION_CLASS)
            .build();
    testFunctions[2] =
        new FunctionBuilder()
            .setDbName(DEFAULT_DATABASE)
            .setName("test_function_hidden_1")
            .setClass(TEST_FUNCTION_CLASS)
            .build();

    client.createDatabase(new DatabaseBuilder().setName(OTHER_DATABASE).build());
    testFunctions[3] =
        new FunctionBuilder()
            .setDbName(OTHER_DATABASE)
            .setName("test_function_to_find_1")
            .setClass(TEST_FUNCTION_CLASS)
            .build();

    // Create the functions, and reload them from the MetaStore
    for(int i=0; i < testFunctions.length; i++) {
      client.createFunction(testFunctions[i]);
      testFunctions[i] = client.getFunction(testFunctions[i].getDbName(),
          testFunctions[i].getFunctionName());
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      client = null;
    }
  }

  /**
   * This test creates and queries a function and then drops it. Good for testing the happy path.
   * @throws Exception
   */
  @Test
  public void testCreateGetDeleteFunction() throws Exception {
    Function function =
        new FunctionBuilder()
            .setDbName(OTHER_DATABASE)
            .setName("test_function")
            .setClass(TEST_FUNCTION_CLASS)
            .setFunctionType(FunctionType.JAVA)
            .setOwnerType(PrincipalType.ROLE)
            .setOwner("owner")
            .setCreateTime(100)
            .addResourceUri(new ResourceUri(ResourceType.JAR, "hdfs:///tmp/jar1.jar"))
            .addResourceUri(new ResourceUri(ResourceType.FILE, "hdfs:///tmp/file1.txt"))
            .addResourceUri(new ResourceUri(ResourceType.ARCHIVE, "hdfs:///tmp/archive1.tgz"))
            .build();

    client.createFunction(function);

    Function createdFunction = client.getFunction(function.getDbName(),
        function.getFunctionName());
    // The createTime will be set on the server side, so the comparison should skip it
    function.setCreateTime(createdFunction.getCreateTime());
    Assert.assertEquals("Comparing functions", function, createdFunction);
    client.dropFunction(function.getDbName(), function.getFunctionName());
    try {
      client.getFunction(function.getDbName(), function.getFunctionName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFunctionDefaultValues() throws Exception {
    Function function = new Function();
    function.setDbName(OTHER_DATABASE);
    function.setFunctionName("test_function");
    function.setClassName(TEST_FUNCTION_CLASS);
    function.setOwnerType(PrincipalType.USER);
    function.setFunctionType(FunctionType.JAVA);

    client.createFunction(function);

    Function createdFunction = client.getFunction(function.getDbName(),
        function.getFunctionName());
    Assert.assertNull("Comparing OwnerName", createdFunction.getOwnerName());
    Assert.assertEquals("Comparing ResourceUris", 0, createdFunction.getResourceUris().size());
    // The create time is set
    Assert.assertNotEquals("Comparing CreateTime", 0, createdFunction.getCreateTime());
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateFunctionNullClass() throws Exception {
    Function function = testFunctions[0];
    function.setClassName(null);

    client.createFunction(function);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateFunctionInvalidName() throws Exception {
    Function function = testFunctions[0];
    function.setFunctionName("test_function_2;");

    client.createFunction(function);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateFunctionEmptyName() throws Exception {
    Function function = testFunctions[0];
    function.setFunctionName("");

    client.createFunction(function);
  }

  @Test
  public void testCreateFunctionNullFunctionName() throws Exception {
    Function function = testFunctions[0];
    function.setFunctionName(null);

    try {
      client.createFunction(function);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testCreateFunctionNullDatabaseName() throws Exception {
    Function function = testFunctions[0];
    function.setDbName(null);

    try {
      client.createFunction(function);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testCreateFunctionNullOwnerType() throws Exception {
    Function function = testFunctions[0];
    function.setFunctionName("test_function_2");
    function.setOwnerType(null);

    try {
      client.createFunction(function);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testCreateFunctionNullFunctionType() throws Exception {
    Function function = testFunctions[0];
    function.setFunctionName("test_function_2");
    function.setFunctionType(null);

    try {
      client.createFunction(function);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test(expected = NoSuchObjectException.class)
  public void testCreateFunctionNoSuchDatabase() throws Exception {
    Function function = testFunctions[0];
    function.setDbName("no_such_database");

    client.createFunction(function);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testCreateFunctionAlreadyExists() throws Exception {
    Function function = testFunctions[0];

    client.createFunction(function);
  }

  @Test
  public void testGetFunctionCaseInsensitive() throws Exception {
    Function function = testFunctions[0];

    // Test in upper case
    Function resultUpper = client.getFunction(function.getDbName().toUpperCase(),
        function.getFunctionName().toUpperCase());
    Assert.assertEquals("Comparing functions", function, resultUpper);

    // Test in mixed case
    Function resultMix = client.getFunction("DeFaUlt", "tEsT_FuncTION_tO_FinD_1");
    Assert.assertEquals("Comparing functions", function, resultMix);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetFunctionNoSuchDatabase() throws Exception {
    // Choosing the 2nd function, since the 1st one is duplicated in the dummy database
    Function function = testFunctions[1];

    client.getFunction("no_such_database", function.getFunctionName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetFunctionNoSuchFunction() throws Exception {
    // Choosing the 2nd function, since the 1st one is duplicated in the dummy database
    Function function = testFunctions[1];

    client.getFunction(function.getDbName(), "no_such_function");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetFunctionNoSuchFunctionInThisDatabase() throws Exception {
    // Choosing the 2nd function, since the 1st one is duplicated in the dummy database
    Function function = testFunctions[1];

    client.getFunction(OTHER_DATABASE, function.getFunctionName());
  }

  @Test
  public void testGetFunctionNullDatabase() throws Exception {
    try {
      client.getFunction(null, OTHER_DATABASE);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws MetaException
      Assert.fail("Expected an NullPointerException or MetaException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (MetaException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test(expected = MetaException.class)
  public void testGetFunctionNullFunctionName() throws Exception {
    client.getFunction(DEFAULT_DATABASE, null);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropFunctionNoSuchDatabase() throws Exception {
    // Choosing the 2nd function, since the 1st one is duplicated in the dummy database
    Function function = testFunctions[1];

    client.dropFunction("no_such_database", function.getFunctionName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropFunctionNoSuchFunction() throws Exception {
    client.dropFunction(DEFAULT_DATABASE, "no_such_function");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropFunctionNoSuchFunctionInThisDatabase() throws Exception {
    // Choosing the 2nd function, since the 1st one is duplicated in the dummy database
    Function function = testFunctions[1];

    client.dropFunction(OTHER_DATABASE, function.getFunctionName());
  }

  @Test
  public void testDropFunctionNullDatabase() throws Exception {
    try {
      client.dropFunction(null, "no_such_function");
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testDropFunctionNullFunctionName() throws Exception {
    try {
      client.dropFunction(DEFAULT_DATABASE, null);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testDropFunctionCaseInsensitive() throws Exception {
    Function function = testFunctions[0];

    // Test in upper case
    client.dropFunction(function.getDbName().toUpperCase(),
        function.getFunctionName().toUpperCase());

    // Check if the function is really removed
    try {
      client.getFunction(function.getDbName(), function.getFunctionName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }

    // Test in mixed case
    client.createFunction(function);
    client.dropFunction("DeFaUlt", "tEsT_FuncTION_tO_FinD_1");

    // Check if the function is really removed
    try {
      client.getFunction(function.getDbName(), function.getFunctionName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }
  }

  @Test
  public void testGetAllFunctions() throws Exception {
    GetAllFunctionsResponse response = client.getAllFunctions();
    List<Function> allFunctions = response.getFunctions();
    Assert.assertEquals("All functions size", 4, allFunctions.size());
    for(Function function : allFunctions) {
      if (function.getDbName().equals(OTHER_DATABASE)) {
        Assert.assertEquals("Comparing functions", testFunctions[3], function);
      } else if (function.getFunctionName().equals("test_function_hidden_1")) {
        Assert.assertEquals("Comparing functions", testFunctions[2], function);
      } else if (function.getFunctionName().equals("test_function_to_find_2")) {
        Assert.assertEquals("Comparing functions", testFunctions[1], function);
      } else {
        Assert.assertEquals("Comparing functions", testFunctions[0], function);
      }
    }

    // Drop one function, see what remains
    client.dropFunction(testFunctions[1].getDbName(), testFunctions[1].getFunctionName());
    response = client.getAllFunctions();
    allFunctions = response.getFunctions();
    Assert.assertEquals("All functions size", 3, allFunctions.size());
    for(Function function : allFunctions) {
      if (function.getDbName().equals(OTHER_DATABASE)) {
        Assert.assertEquals("Comparing functions", testFunctions[3], function);
      } else if (function.getFunctionName().equals("test_function_hidden_1")) {
        Assert.assertEquals("Comparing functions", testFunctions[2], function);
      } else {
        Assert.assertEquals("Comparing functions", testFunctions[0], function);
      }
    }
  }

  @Test
  public void testGetFunctions() throws Exception {
    // Find functions which name contains _to_find_ in the default database
    List<String> functions = client.getFunctions(DEFAULT_DATABASE, "*_to_find_*");
    Assert.assertEquals("Found functions size", 2, functions.size());
    Assert.assertTrue("Should contain", functions.contains("test_function_to_find_1"));
    Assert.assertTrue("Should contain", functions.contains("test_function_to_find_2"));

    // Find functions which name contains _to_find_ or _hidden_ in the default database
    functions = client.getFunctions(DEFAULT_DATABASE, "*_to_find_*|*_hidden_*");
    Assert.assertEquals("Found functions size", 3, functions.size());
    Assert.assertTrue("Should contain", functions.contains("test_function_to_find_1"));
    Assert.assertTrue("Should contain", functions.contains("test_function_to_find_2"));
    Assert.assertTrue("Should contain", functions.contains("test_function_hidden_1"));

    // Find functions which name contains _to_find_ in the dummy database
    functions = client.getFunctions(OTHER_DATABASE, "*_to_find_*");
    Assert.assertEquals("Found functions size", 1, functions.size());
    Assert.assertTrue("Should contain", functions.contains("test_function_to_find_1"));

    // Look for functions but do not find any
    functions = client.getFunctions(DEFAULT_DATABASE, "*_not_such_function_*");
    Assert.assertEquals("No such functions size", 0, functions.size());

    // Look for functions without pattern
    functions = client.getFunctions(DEFAULT_DATABASE, null);
    Assert.assertEquals("Search functions without pattern size", 3, functions.size());

    // Look for functions with empty pattern
    functions = client.getFunctions(DEFAULT_DATABASE, "");
    Assert.assertEquals("Search functions with empty pattern", 0, functions.size());

    // No such database
    functions = client.getFunctions("no_such_database", "*_to_find_*");
    Assert.assertEquals("No such functions size", 0, functions.size());
  }

  @Test
  public void testGetFunctionsCaseInsensitive() throws Exception {
    // Check case insensitive search
    List<String> functions = client.getFunctions("deFAulT", "*_tO_FiND*");
    Assert.assertEquals("Found functions size", 2, functions.size());
    Assert.assertTrue("Should contain", functions.contains("test_function_to_find_1"));
    Assert.assertTrue("Should contain", functions.contains("test_function_to_find_2"));
  }

  @Test(expected = MetaException.class)
  public void testGetFunctionsNullDatabase() throws Exception {
    client.getFunctions(null, OTHER_DATABASE);
  }

  @Test
  public void testAlterFunction() throws Exception {
    Function newFunction =
        new FunctionBuilder()
            .setDbName(OTHER_DATABASE)
            .setName("test_function_2")
            .setOwner("Owner2")
            .setOwnerType(PrincipalType.GROUP)
            .setClass("org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper2")
            .setFunctionType(FunctionType.JAVA)
            .build();

    client.alterFunction(testFunctions[0].getDbName(), testFunctions[0].getFunctionName(),
        newFunction);

    Function alteredFunction = client.getFunction(newFunction.getDbName(),
        newFunction.getFunctionName());
    // Currently this method only sets
    //  - Database
    //  - FunctionName
    //  - OwnerName
    //  - OwnerType
    //  - ClassName
    //  - FunctionType
    Assert.assertEquals("Comparing Database", newFunction.getDbName(),
        alteredFunction.getDbName());
    Assert.assertEquals("Comparing FunctionName", newFunction.getFunctionName(),
        alteredFunction.getFunctionName());
    Assert.assertEquals("Comparing OwnerName", newFunction.getOwnerName(),
        alteredFunction.getOwnerName());
    Assert.assertEquals("Comparing OwnerType", newFunction.getOwnerType(),
        alteredFunction.getOwnerType());
    Assert.assertEquals("Comparing ClassName", newFunction.getClassName(),
        alteredFunction.getClassName());
    Assert.assertEquals("Comparing FunctionType", newFunction.getFunctionType(),
        alteredFunction.getFunctionType());
    try {
      client.getFunction(testFunctions[0].getDbName(), testFunctions[0].getDbName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }

    // Test that not changing the database and the function name, but only other parameters, like
    // function class will not cause Exception
    newFunction = testFunctions[1].deepCopy();
    newFunction.setClassName("NewClassName");

    client.alterFunction(testFunctions[1].getDbName(), testFunctions[1].getFunctionName(),
        newFunction);

    alteredFunction = client.getFunction(newFunction.getDbName(), newFunction.getFunctionName());
    Assert.assertEquals("Comparing functions", newFunction, alteredFunction);
  }

  private Function getNewFunction() throws MetaException {
    return new FunctionBuilder()
            .setName("test_function_2")
            .setClass(TEST_FUNCTION_CLASS)
            .build();
  }

  @Test(expected = MetaException.class)
  public void testAlterFunctionNoSuchDatabase() throws Exception {
    // Choosing the 2nd function, since the 1st one is duplicated in the dummy database
    Function originalFunction = testFunctions[1];
    Function newFunction = getNewFunction();

    client.alterFunction("no_such_database", originalFunction.getFunctionName(), newFunction);
  }

  @Test(expected = MetaException.class)
  public void testAlterFunctionNoSuchFunction() throws Exception {
    // Choosing the 2nd function, since the 1st one is duplicated in the dummy database
    Function originalFunction = testFunctions[1];
    Function newFunction = getNewFunction();

    client.alterFunction(originalFunction.getDbName(), "no_such_function", newFunction);
  }

  @Test(expected = MetaException.class)
  public void testAlterFunctionNoSuchFunctionInThisDatabase() throws Exception {
    // Choosing the 2nd function, since the 1st one is duplicated in the dummy database
    Function originalFunction = testFunctions[1];
    Function newFunction = getNewFunction();

    client.alterFunction(OTHER_DATABASE, originalFunction.getFunctionName(), newFunction);
  }

  @Test
  public void testAlterFunctionNullDatabase() throws Exception {
    Function newFunction = getNewFunction();

    try {
      client.alterFunction(null, OTHER_DATABASE, newFunction);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testAlterFunctionNullFunctionName() throws Exception {
    Function newFunction = getNewFunction();

    try {
      client.alterFunction(DEFAULT_DATABASE, null, newFunction);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testAlterFunctionNullFunction() throws Exception {
    Function originalFunction = testFunctions[1];

    try {
      client.alterFunction(DEFAULT_DATABASE, originalFunction.getFunctionName(), null);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testAlterFunctionInvalidNameInNew() throws Exception {
    Function newFunction = getNewFunction();
    newFunction.setFunctionName("test_function_2;");

    try {
      client.alterFunction(DEFAULT_DATABASE, "test_function_to_find_2", newFunction);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // InvalidObjectException, remote throws TApplicationException
      Assert.fail("Expected an InvalidObjectException or TApplicationException to be thrown");
    } catch (InvalidObjectException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TApplicationException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testAlterFunctionEmptyNameInNew() throws Exception {
    Function newFunction = getNewFunction();
    newFunction.setFunctionName("");

    try {
      client.alterFunction(DEFAULT_DATABASE, "test_function_to_find_2", newFunction);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // InvalidObjectException, remote throws TApplicationException
      Assert.fail("Expected an InvalidObjectException or TApplicationException to be thrown");
    } catch (InvalidObjectException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TApplicationException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testAlterFunctionNullClassInNew() throws Exception {
    Function newFunction = getNewFunction();
    newFunction.setClassName(null);

    try {
      client.alterFunction(DEFAULT_DATABASE, "test_function_to_find_2", newFunction);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // InvalidObjectException, remote throws TApplicationException
      Assert.fail("Expected an InvalidObjectException or TApplicationException to be thrown");
    } catch (InvalidObjectException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TApplicationException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testAlterFunctionNullFunctionNameInNew() throws Exception {
    Function newFunction = getNewFunction();
    newFunction.setFunctionName(null);

    try {
      client.alterFunction(DEFAULT_DATABASE, "test_function_to_find_2", newFunction);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testAlterFunctionNullDatabaseNameInNew() throws Exception {
    Function newFunction = getNewFunction();
    newFunction.setDbName(null);

    try {
      client.alterFunction(DEFAULT_DATABASE, "test_function_to_find_2", newFunction);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testAlterFunctionNullOwnerTypeInNew() throws Exception {
    Function newFunction = getNewFunction();
    newFunction.setOwnerType(null);

    try {
      client.alterFunction(DEFAULT_DATABASE, "test_function_to_find_2", newFunction);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testAlterFunctionNullFunctionTypeInNew() throws Exception {
    Function newFunction = getNewFunction();
    newFunction.setFunctionType(null);

    try {
      client.alterFunction(DEFAULT_DATABASE, "test_function_to_find_2", newFunction);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // NullPointerException, remote throws TTransportException
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testAlterFunctionNoSuchDatabaseInNew() throws Exception {
    Function newFunction = getNewFunction();
    newFunction.setDbName("no_such_database");

    try {
      client.alterFunction(DEFAULT_DATABASE, "test_function_to_find_2", newFunction);
      // TODO: Should have a check on the server side. Embedded metastore throws
      // InvalidObjectException, remote throws TApplicationException
      Assert.fail("Expected an InvalidObjectException or TApplicationException to be thrown");
    } catch (InvalidObjectException exception) {
      // Expected exception - Embedded MetaStore
      exception.printStackTrace();
    } catch (TApplicationException exception) {
      // Expected exception - Remote MetaStore
      exception.printStackTrace();
    }
  }

  @Test(expected = MetaException.class)
  public void testAlterFunctionAlreadyExists() throws Exception {
    Function originalFunction = testFunctions[0];
    Function newFunction = testFunctions[1];

    client.alterFunction(originalFunction.getDbName(), originalFunction.getFunctionName(),
        newFunction);
  }

  @Test
  public void testAlterFunctionCaseInsensitive() throws Exception {
    Function newFunction =
        new FunctionBuilder()
            .setDbName(OTHER_DATABASE)
            .setName("test_function_2")
            .setClass(TEST_FUNCTION_CLASS)
            .build();
    Function originalFunction = testFunctions[1];

    // Test in upper case
    client.alterFunction(originalFunction.getDbName().toUpperCase(),
        originalFunction.getFunctionName().toUpperCase(), newFunction);
    Function alteredFunction = client.getFunction(newFunction.getDbName(),
        newFunction.getFunctionName());

    // The creation time is changed, so we do not check that
    newFunction.setCreateTime(alteredFunction.getCreateTime());
    Assert.assertEquals("Comparing functions", newFunction, alteredFunction);
    try {
      client.getFunction(originalFunction.getDbName(), originalFunction.getDbName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }

    // Test in mixed case
    originalFunction = testFunctions[2];
    newFunction.setFunctionName("test_function_3");
    client.alterFunction("DeFaUlt", "tEsT_FuncTION_HiDDEn_1", newFunction);
    alteredFunction = client.getFunction(newFunction.getDbName(), newFunction.getFunctionName());

    // The creation time is changed, so we do not check that
    newFunction.setCreateTime(alteredFunction.getCreateTime());
    Assert.assertEquals("Comparing functions", newFunction, alteredFunction);
    try {
      client.getFunction(originalFunction.getDbName(), originalFunction.getDbName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }
  }
}
