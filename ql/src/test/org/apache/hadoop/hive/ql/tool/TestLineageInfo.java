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

package org.apache.hadoop.hive.ql.tool;

import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.tools.LineageInfo;

/**
 * TestLineageInfo.
 *
 */
public class TestLineageInfo extends TestCase {

  /**
   * Checks whether the test outputs match the expected outputs.
   * 
   * @param lep
   *          The LineageInfo extracted from the test
   * @param i
   *          The set of input tables
   * @param o
   *          The set of output tables
   */
  private void checkOutput(LineageInfo lep, TreeSet<String> i, TreeSet<String> o) {

    if (!i.equals(lep.getInputTableList())) {
      fail("Input table not same");
    }
    if (!o.equals(lep.getOutputTableList())) {
      fail("Output table not same");
    }
  }

  public void testSimpleQuery() {
    LineageInfo lep = new LineageInfo();
    try {
      lep.getLineageInfo("INSERT OVERWRITE TABLE dest1 partition (ds = '111')  " 
          + "SELECT s.* FROM srcpart TABLESAMPLE (BUCKET 1 OUT OF 1) s " 
          + "WHERE s.ds='2008-04-08' and s.hr='11'");
      TreeSet<String> i = new TreeSet<String>();
      TreeSet<String> o = new TreeSet<String>();
      i.add("srcpart");
      o.add("dest1");
      checkOutput(lep, i, o);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed");
    }
  }

  public void testSimpleQuery2() {
    LineageInfo lep = new LineageInfo();
    try {
      lep.getLineageInfo("FROM (FROM src select src.key, src.value " 
          + "WHERE src.key < 10 UNION ALL FROM src SELECT src.* WHERE src.key > 10 ) unioninput " 
          + "INSERT OVERWRITE DIRECTORY '../../../../build/contrib/hive/ql/test/data/warehouse/union.out' " 
          + "SELECT unioninput.*");
      TreeSet<String> i = new TreeSet<String>();
      TreeSet<String> o = new TreeSet<String>();
      i.add("src");
      checkOutput(lep, i, o);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed");
    }
  }

  public void testSimpleQuery3() {
    LineageInfo lep = new LineageInfo();
    try {
      lep.getLineageInfo("FROM (FROM src select src.key, src.value " 
          + "WHERE src.key < 10 UNION ALL FROM src1 SELECT src1.* WHERE src1.key > 10 ) unioninput " 
          + "INSERT OVERWRITE DIRECTORY '../../../../build/contrib/hive/ql/test/data/warehouse/union.out' " 
          + "SELECT unioninput.*");
      TreeSet<String> i = new TreeSet<String>();
      TreeSet<String> o = new TreeSet<String>();
      i.add("src");
      i.add("src1");
      checkOutput(lep, i, o);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed");
    }
  }

  public void testSimpleQuery4() {
    LineageInfo lep = new LineageInfo();
    try {
      lep.getLineageInfo("FROM ( FROM ( FROM src1 src1 SELECT src1.key AS c1, src1.value AS c2 WHERE src1.key > 10 and src1.key < 20) a RIGHT OUTER JOIN ( FROM src2 src2 SELECT src2.key AS c3, src2.value AS c4 WHERE src2.key > 15 and src2.key < 25) b ON (a.c1 = b.c3) SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4) c SELECT c.c1, c.c2, c.c3, c.c4");
      TreeSet<String> i = new TreeSet<String>();
      TreeSet<String> o = new TreeSet<String>();
      i.add("src1");
      i.add("src2");
      checkOutput(lep, i, o);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed");
    }
  }

  public void testSimpleQuery5() {
    LineageInfo lep = new LineageInfo();
    try {
      lep.getLineageInfo("insert overwrite table x select a.y, b.y " 
          + "from a a full outer join b b on (a.x = b.y)");
      TreeSet<String> i = new TreeSet<String>();
      TreeSet<String> o = new TreeSet<String>();
      i.add("a");
      i.add("b");
      o.add("x");
      checkOutput(lep, i, o);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed");
    }
  }
}
