/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.accumulo;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.serde.AccumuloIndexParameters;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestAccumuloIndexParameters {

  public static class MockAccumuloIndexScanner implements AccumuloIndexScanner {

    @Override
    public void init(Configuration conf) {
    }

    @Override
    public boolean isIndexed(String columnName) {
      return false;
    }

    @Override
    public List<Range> getIndexRowRanges(String column, Range indexRange) {
      return null;
    }
  }

  @Test
  public void testDefaultScanner() {
    try {
      AccumuloIndexScanner scanner = new AccumuloIndexParameters(new Configuration()).createScanner();
      assertTrue(scanner instanceof AccumuloDefaultIndexScanner);
    } catch (AccumuloIndexScannerException e) {
      fail("Unexpected exception thrown");
    }
  }

  @Test
  public void testUserHandler() throws AccumuloIndexScannerException {
    Configuration conf = new Configuration();
    conf.set(AccumuloIndexParameters.INDEX_SCANNER, MockAccumuloIndexScanner.class.getName());
    AccumuloIndexScanner scanner = new AccumuloIndexParameters(conf).createScanner();
    assertTrue(scanner instanceof MockAccumuloIndexScanner);
  }

  @Test
  public void testBadHandler() {
    Configuration conf = new Configuration();
    conf.set(AccumuloIndexParameters.INDEX_SCANNER, "a.class.does.not.exist.IndexHandler");
    try {
      AccumuloIndexScanner scanner = new AccumuloIndexParameters(conf).createScanner();
    } catch (AccumuloIndexScannerException e) {
      return;
    }
    fail("Failed to throw exception for class not found");
  }

  @Test
  public void getIndexColumns() {
    Configuration conf = new Configuration();
    conf.set(AccumuloIndexParameters.INDEXED_COLUMNS, "a,b,c");
    Set<String> cols = new AccumuloIndexParameters(conf).getIndexColumns();
    assertEquals(3, cols.size());
    assertTrue("Missing column a", cols.contains("a"));
    assertTrue("Missing column b", cols.contains("b"));
    assertTrue("Missing column c", cols.contains("c"));
  }

  @Test
  public void getMaxIndexRows() {
    Configuration conf = new Configuration();
    conf.setInt(AccumuloIndexParameters.MAX_INDEX_ROWS, 10);
    int maxRows = new AccumuloIndexParameters(conf).getMaxIndexRows();
    assertEquals(10, maxRows);
  }

  @Test
  public void getAuths() {
    Configuration conf = new Configuration();
    conf.set(AccumuloSerDeParameters.AUTHORIZATIONS_KEY, "public,open");
    Authorizations auths = new AccumuloIndexParameters(conf).getTableAuths();
    assertEquals(2, auths.size());
    assertTrue("Missing auth public", auths.contains("public"));
    assertTrue("Missing auth open", auths.contains("open"));
  }

}
