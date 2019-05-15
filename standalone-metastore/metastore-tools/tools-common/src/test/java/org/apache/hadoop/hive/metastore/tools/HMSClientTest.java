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

package org.apache.hadoop.hive.metastore.tools;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.tools.HMSClient;
import org.apache.hadoop.hive.metastore.tools.Util;
import org.apache.thrift.TException;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.util.Set;

import static org.apache.hadoop.hive.metastore.tools.Util.getServerUri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HMSClientTest {
  private static final String PARAM_NAME = "param";
  private static final String VALUE_NAME = "value";
  private static final String TEST_DATABASE="hmsClientTest";
  private static final String TEST_DATABASE_DESCRIPTION="hmsclienttest description";
  private static final ImmutableMap<String, String> TEST_DATABASE_PARAMS =
      new ImmutableMap.Builder<String, String>()
      .put(PARAM_NAME, VALUE_NAME)
      .build();
  private static boolean hasClient = false;

  private static final String TEST_TABLE_NAME="test1";
  private static final Table TEST_TABLE =
      Util.TableBuilder.buildDefaultTable(TEST_DATABASE, TEST_TABLE_NAME);

  private static HMSClient client = null;

  @BeforeAll
  static void init() throws Exception {
    Database db = new Util.DatabaseBuilder(TEST_DATABASE)
        .withDescription(TEST_DATABASE_DESCRIPTION)
        .withParams(TEST_DATABASE_PARAMS)
        .build();
    // Create client and default test database
    try {
      client =
          new HMSClient(getServerUri(null, null), null);
          client.createDatabase(db);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }

  @AfterAll
  static void shutdown() throws TException {
    if (client != null) {
      // Destroy test database
      client.dropDatabase(TEST_DATABASE);
    }
  }

  @Before
  public void beforeTest() {
    Assume.assumeTrue(client != null);
  }

  /**
   * Verify that list of databases contains "default" and test database
   * @throws Exception
   */
  @Test
  public void getAllDatabases() throws Exception {
    Set<String> databases = client.getAllDatabases(null);
    MatcherAssert.assertThat(databases, Matchers.hasItem("default"));
    MatcherAssert.assertThat(databases, Matchers.hasItem(TEST_DATABASE.toLowerCase()));
    assertThat(client.getAllDatabases(TEST_DATABASE.toLowerCase()), Matchers.contains(TEST_DATABASE.toLowerCase()));
  }

  /**
   * Verify that an attempt to create an existing database throws AlreadyExistsException.
   */
  @Test
  public void createExistingDatabase() {
    Throwable exception = Assertions.assertThrows(AlreadyExistsException.class,
        () -> client.createDatabase(TEST_DATABASE));
  }

  /**
   * Creating a database with null name should not be allowed
   * and should throw MetaException.
   */
  @Test
  public void createDatabaseNullName() {
    Database db = new Util.DatabaseBuilder(TEST_DATABASE)
        .build();
    db.setName(null);
    Throwable exception = Assertions.assertThrows(MetaException.class,
        () -> client.createDatabase(db));
  }

  /**
   * Creating a database with an empty name should not be allowed
   * and should throw InvalidObjectException
   */
  @Test
  public void createDatabaseEmptyName() {
    Assume.assumeTrue(client != null);
    Database db = new Util.DatabaseBuilder(TEST_DATABASE)
        .build();
    db.setName("");
    Throwable exception = Assertions.assertThrows(InvalidObjectException.class,
        () -> client.createDatabase(db));
  }

  /**
   * Verify that getDatabase() returns all expected fields
   * @throws TException if fails to get database info
   */
  @Test
  public void getDatabase() throws TException {
    Database db = client.getDatabase(TEST_DATABASE);
    MatcherAssert.assertThat(db.getName(), Matchers.equalToIgnoringCase(TEST_DATABASE));
    MatcherAssert.assertThat(db.getDescription(), Matchers.equalTo(TEST_DATABASE_DESCRIPTION));
    MatcherAssert.assertThat(db.getParameters(), Matchers.equalTo(TEST_DATABASE_PARAMS));
    MatcherAssert.assertThat(db.getLocationUri(), Matchers.containsString(TEST_DATABASE.toLowerCase()));
  }

  /**
   * Verify that locating database is case-insensitive
   */
  @Test
  public void getDatabaseCI() throws TException {
    Database db = client.getDatabase(TEST_DATABASE.toUpperCase());
    MatcherAssert.assertThat(db.getName(), Matchers.equalToIgnoringCase(TEST_DATABASE));
    MatcherAssert.assertThat(db.getDescription(), Matchers.equalTo(TEST_DATABASE_DESCRIPTION));
    MatcherAssert.assertThat(db.getParameters(), Matchers.equalTo(TEST_DATABASE_PARAMS));
    MatcherAssert.assertThat(db.getLocationUri(), Matchers.containsString(TEST_DATABASE.toLowerCase()));
  }

  /**
   * Verify that searching for non-existing database throws
   * NoSuchObjectException
   */
  @Test
  public void getNonExistingDb() {
    Throwable exception = Assertions.assertThrows(NoSuchObjectException.class,
        () -> client.getDatabase("WhatIsThisDatabase"));
  }


  /**
   * Verify that dropping for non-existing database throws
   * NoSuchObjectException
   */
  @Test
  public void dropNonExistingDb() {
    Throwable exception = Assertions.assertThrows(NoSuchObjectException.class,
        () -> client.dropDatabase("WhatIsThisDatabase"));
  }

  @Test
  public void getAllTables() throws TException {
    try {
      client.createTable(TEST_TABLE);
      assertThat(client.getAllTables(TEST_DATABASE, null), Matchers.contains(TEST_TABLE_NAME));
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    } finally {
      client.dropTable(TEST_DATABASE, TEST_TABLE_NAME);
    }
  }

}