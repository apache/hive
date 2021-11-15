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

package org.apache.hadoop.hive.metastore.tools.metatool;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for HiveMetaToolCommandLine. */
@Category(MetastoreUnitTest.class)
public class TestHiveMetaToolCommandLine {
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testParseListFSRoot() throws ParseException {
    HiveMetaToolCommandLine cl = new HiveMetaToolCommandLine(new String[] {"-listFSRoot"});
    assertTrue(cl.isListFSRoot());
    assertFalse(cl.isExecuteJDOQL());
    assertNull(cl.getJDOQLQuery());
    assertFalse(cl.isUpdateLocation());
    assertNull(cl.getUpddateLocationParams());
    assertFalse(cl.isListExtTblLocs());
    assertNull(cl.getListExtTblLocsParams());
    assertFalse(cl.isDryRun());
    assertNull(cl.getSerdePropKey());
    assertNull(cl.getTablePropKey());
  }

  @Test
  public void testParseExecuteJDOQL() throws ParseException {
    HiveMetaToolCommandLine cl = new HiveMetaToolCommandLine(new String[] {"-executeJDOQL", "select a from b"});
    assertFalse(cl.isListFSRoot());
    assertTrue(cl.isExecuteJDOQL());
    assertEquals("select a from b", cl.getJDOQLQuery());
    assertFalse(cl.isUpdateLocation());
    assertNull(cl.getUpddateLocationParams());
    assertFalse(cl.isListExtTblLocs());
    assertNull(cl.getListExtTblLocsParams());
    assertFalse(cl.isDryRun());
    assertNull(cl.getSerdePropKey());
    assertNull(cl.getTablePropKey());
  }

  @Test
  public void testParseUpdateLocation() throws ParseException {
    String[] args = new String[] {"-updateLocation", "hdfs://new.loc", "hdfs://old.loc", "-dryRun", "-serdePropKey",
        "abc", "-tablePropKey", "def"};
    HiveMetaToolCommandLine cl = new HiveMetaToolCommandLine(args);
    assertFalse(cl.isListFSRoot());
    assertFalse(cl.isExecuteJDOQL());
    assertNull(cl.getJDOQLQuery());
    assertTrue(cl.isUpdateLocation());
    assertEquals("hdfs://new.loc", cl.getUpddateLocationParams()[0]);
    assertEquals("hdfs://old.loc", cl.getUpddateLocationParams()[1]);
    assertFalse(cl.isListExtTblLocs());
    assertNull(cl.getListExtTblLocsParams());
    assertTrue(cl.isDryRun());
    assertEquals("abc", cl.getSerdePropKey());
    assertEquals("def", cl.getTablePropKey());
  }

  @Test
  public void testNoTask() throws ParseException {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("exactly one of -listFSRoot, -executeJDOQL, -updateLocation, -listExtTblLocs, -diffExtTblLocs must be set");

    new HiveMetaToolCommandLine(new String[] {});
  }

  @Test
  public void testMultipleTask() throws ParseException {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("exactly one of -listFSRoot, -executeJDOQL, -updateLocation, -listExtTblLocs, -diffExtTblLocs must be set");

    new HiveMetaToolCommandLine(new String[] {"-listFSRoot", "-executeJDOQL", "select a from b"});
  }

  @Test
  public void testUpdateLocationOneArgument() throws ParseException {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("HiveMetaTool:updateLocation takes in 2 arguments but was passed 1 arguments");

    new HiveMetaToolCommandLine(new String[] {"-updateLocation", "hdfs://abc.de"});
  }

  @Test
  public void testListExtTblLocsOneArgument() throws ParseException {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("HiveMetaTool:listExtTblLocs takes in 2 arguments but was passed 1 arguments");

    new HiveMetaToolCommandLine(new String[] {"-listExtTblLocs", "db1"});
  }

  @Test
  public void testDiffExtTblLocsArgCount() throws ParseException {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("HiveMetaTool:diffExtTblLocs takes in 3 arguments but was passed 1 arguments");
    new HiveMetaToolCommandLine(new String[] {"-diffExtTblLocs", "file1"});

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("HiveMetaTool:diffExtTblLocs takes in 3 arguments but was passed 2 arguments");
    new HiveMetaToolCommandLine(new String[] {"-diffExtTblLocs", "file1", "file2"});

  }

  @Test
  public void testDryRunNotAllowed() throws ParseException {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("-dryRun, -serdePropKey, -tablePropKey may be used only for the -updateLocation command");

    new HiveMetaToolCommandLine(new String[] {"-listFSRoot", "-dryRun"});
  }

  @Test
  public void testSerdePropKeyNotAllowed() throws ParseException {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("-dryRun, -serdePropKey, -tablePropKey may be used only for the -updateLocation command");

    new HiveMetaToolCommandLine(new String[] {"-listFSRoot", "-serdePropKey", "abc"});
  }

  @Test
  public void testTablePropKeyNotAllowed() throws ParseException {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("-dryRun, -serdePropKey, -tablePropKey may be used only for the -updateLocation command");

    new HiveMetaToolCommandLine(new String[] {"-executeJDOQL", "select a from b", "-tablePropKey", "abc"});
  }
}
