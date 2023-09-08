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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

@Category(MetastoreUnitTest.class)
public class TestPartFilterExprUtil {
  @Test
  public void testAndOrPrecedence() throws MetaException {
    checkFilter("dt=10 or dt=20 and dt=30 and dt=40 or dt=50 or dt=60 and dt=70",
            "TreeNode{lhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=10}, andOr='OR', rhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=20}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=30}}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=40}}}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=50}}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=60}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=70}}}");
    checkFilter("dt=10 or dt=20 and (dt=30 and dt=40 or dt=50) or dt=60 and dt=70",
            "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=10}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=20}, andOr='AND', rhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=30}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=40}}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=50}}}}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=60}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=70}}}");
    checkFilter("dt=10 or dt=20 and dt=30 and (dt=40 or dt=50 or dt=60) and dt=70",
            "TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=10}, andOr='OR', rhs=TreeNode{lhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=20}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=30}}, andOr='AND', rhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=40}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=50}}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=60}}}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=70}}}");
    checkFilter("(dt=10 or dt=20) and dt=30 and (dt=40 or dt=50) or dt=60 and dt=70",
            "TreeNode{lhs=TreeNode{lhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=10}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=20}}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=30}}, andOr='AND', rhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=40}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=50}}}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=60}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=70}}}");
    checkFilter("(dt=10 or dt=20) and dt=30 and ((dt=40 or dt=50) or dt=60) and dt=70",
            "TreeNode{lhs=TreeNode{lhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=10}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=20}}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=30}}, andOr='AND', rhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=40}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=50}}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=60}}}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=70}}");
    checkFilter("(dt=10 or dt=20) and (dt=30 and ((dt=40 or dt=50) or dt=60) and dt=70)",
            "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=10}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=20}}, andOr='AND', rhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=30}, andOr='AND', rhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=40}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=50}}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=60}}}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=70}}}");
    checkFilter("dt=10 or dt=20 and (dt=30 and ((dt=40 or dt=50) or dt=60) and dt=70)",
            "TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=10}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=20}, andOr='AND', rhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=30}, andOr='AND', rhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=40}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=50}}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=60}}}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=70}}}}");
    checkFilter("dt=10 or (dt=20 and dt=30 and (dt=40 or dt=50) or dt=60) and dt=70",
            "TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=10}, andOr='OR', rhs=TreeNode{lhs=TreeNode{lhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=20}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=30}}, andOr='AND', rhs=TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=40}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=50}}}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=60}}, andOr='AND', rhs=LeafNode{keyName='dt', operator='=', value=70}}}");
  }

  @Test
  public void testExpressionCombination() throws MetaException {
    checkFilter("(a) in (10, 20) and a != 30",
            "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='a', operator='=', value=10}, andOr='OR', rhs=LeafNode{keyName='a', operator='=', value=20}}, andOr='AND', rhs=LeafNode{keyName='a', operator='!=', value=30}}");
    checkFilter("(a) in (10, 20) and a between 10 and 15",
            "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='a', operator='=', value=10}, andOr='OR', rhs=LeafNode{keyName='a', operator='=', value=20}}, andOr='AND', rhs=TreeNode{lhs=LeafNode{keyName='a', operator='>=', value=10}, andOr='AND', rhs=LeafNode{keyName='a', operator='<=', value=15}}}");
    checkFilter("(a) in (10, 20) and a not between 10 and 15",
            "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='a', operator='=', value=10}, andOr='OR', rhs=LeafNode{keyName='a', operator='=', value=20}}, andOr='AND', rhs=TreeNode{lhs=LeafNode{keyName='a', operator='<', value=10}, andOr='OR', rhs=LeafNode{keyName='a', operator='>', value=15}}}");
    checkFilter("(a) in (10, 20) and a not between 10 and 15 and b < 10",
            "TreeNode{lhs=TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='a', operator='=', value=10}, andOr='OR', rhs=LeafNode{keyName='a', operator='=', value=20}}, andOr='AND', rhs=TreeNode{lhs=LeafNode{keyName='a', operator='<', value=10}, andOr='OR', rhs=LeafNode{keyName='a', operator='>', value=15}}}, andOr='AND', rhs=LeafNode{keyName='b', operator='<', value=10}}");
  }

  @Test
  public void testSingleColInExpressionWhenDateLiteralTypeIsNotSpecifiedNorQuoted() throws MetaException {
    checkFilter("(j) in (1990-11-10, 1990-11-11, 1990-11-12)",
    "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-10}, andOr='OR', rhs=LeafNode{keyName='j', operator='=', value=1990-11-11}}, andOr='OR', rhs=LeafNode{keyName='j', operator='=', value=1990-11-12}}");
  }

  @Test
  public void testDateColumnNameKeyword() throws MetaException {
    MetaException exception = assertThrows(MetaException.class,
            () -> PartFilterExprUtil.parseFilterTree("date='1990-11-10'"));

    assertTrue(exception.getMessage().contains("Error parsing partition filter"));
  }

  @Test
  public void testDateColumnNameKeywordWithBackTicks() throws MetaException {
    checkFilter("`date`='1990-11-10'",
    "LeafNode{keyName='date', operator='=', value=1990-11-10}");
  }

  @Test
  public void testSingleColInExpressionWhenDateLiteralTypeIsSpecified() throws MetaException {
    checkFilter("(j) IN (DATE'1990-11-10', DATE'1990-11-11', DATE'1990-11-12')",
    "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-10}, andOr='OR', rhs=LeafNode{keyName='j', operator='=', value=1990-11-11}}, andOr='OR', rhs=LeafNode{keyName='j', operator='=', value=1990-11-12}}");
  }

  @Test
  public void testMultiColInExpressionWhenDateLiteralTypeIsNotSpecifiedNorQuoted() throws MetaException {
    checkFilter("(struct(ds1,ds2)) IN (struct(2000-05-08, 2001-04-08), struct(2000-05-09, 2001-04-09))",
    "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-08}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-08}}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-09}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-09}}}");
  }

  @Test
  public void testMultiColInExpressionWhenDateLiteralTypeIsSpecified() throws MetaException {
    checkFilter("(struct(ds1,ds2)) IN (struct(DATE'2000-05-08',DATE'2001-04-08'), struct(DATE'2000-05-09',DATE'2001-04-09'))",
    "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-08}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-08}}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-09}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-09}}}");
  }

  @Test
  public void testSingleColInExpressionWhenTimestampLiteralTypeIsNotSpecifiedNorQuoted() throws MetaException {
    checkFilter("(dt) IN (2000-01-01 01:00:00, 2000-01-01 01:42:00)",
    "TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=2000-01-01 01:00:00.0}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=2000-01-01 01:42:00.0}}");
  }

  @Test
  public void testSingleColInExpressionWhenTimestampLiteralTypeIsSpecified() throws MetaException {
    checkFilter("(j) IN (TIMESTAMP'2000-01-01 01:00:00', TIMESTAMP'2000-01-01 01:42:00')",
    "TreeNode{lhs=LeafNode{keyName='j', operator='=', value=2000-01-01 01:00:00.0}, andOr='OR', rhs=LeafNode{keyName='j', operator='=', value=2000-01-01 01:42:00.0}}");
  }

  @Test
  public void testMultiColInExpressionWhenTimestampLiteralTypeIsNotSpecifiedNorQuoted() throws MetaException {
    checkFilter("(struct(ds1,ds2)) IN (struct(2000-05-08 01:00:00, 2001-04-08 01:00:00), struct(2000-05-09 01:00:00, 2001-04-09 01:00:00))",
    "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-08 01:00:00.0}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-08 01:00:00.0}}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-09 01:00:00.0}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-09 01:00:00.0}}}");
  }

  @Test
  public void testMultiColInExpressionWhenTimestampLiteralTypeIsSpecified() throws MetaException {
    checkFilter("(struct(ds1,ds2)) IN (struct(TIMESTAMP'2000-05-08 01:00:00',TIMESTAMP'2001-04-08 01:00:00'), struct(TIMESTAMP'2000-05-09 01:00:00',TIMESTAMP'2001-04-09 01:00:00'))",
    "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-08 01:00:00.0}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-08 01:00:00.0}}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-09 01:00:00.0}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-09 01:00:00.0}}}");
  }

  @Test
  public void testBetweenExpressionWhenDateLiteralTypeIsNotSpecifiedNorQuoted() throws MetaException {
    checkFilter("(j BETWEEN 1990-11-10 AND 1990-11-11)",
    "TreeNode{lhs=LeafNode{keyName='j', operator='>=', value=1990-11-10}, andOr='AND', rhs=LeafNode{keyName='j', operator='<=', value=1990-11-11}}");
  }

  @Test
  public void testBetweenExpressionWhenDateLiteralTypeIsSpecified() throws MetaException {
    checkFilter("(j BETWEEN DATE'1990-11-10' AND DATE'1990-11-11')",
    "TreeNode{lhs=LeafNode{keyName='j', operator='>=', value=1990-11-10}, andOr='AND', rhs=LeafNode{keyName='j', operator='<=', value=1990-11-11}}");
  }

  @Test
  public void testBetweenExpressionWhenTimestampLiteralTypeIsNotSpecifiedNorQuoted() throws MetaException {
    checkFilter("dt BETWEEN 2000-01-01 01:00:00 AND 2000-01-01 01:42:00)",
    "TreeNode{lhs=LeafNode{keyName='dt', operator='>=', value=2000-01-01 01:00:00.0}, andOr='AND', rhs=LeafNode{keyName='dt', operator='<=', value=2000-01-01 01:42:00.0}}");
  }

  @Test
  public void testBetweenExpressionWhenTimestampLiteralTypeIsSpecified() throws MetaException {
    checkFilter("dt BETWEEN TIMESTAMP'2000-01-01 01:00:00' AND TIMESTAMP'2000-01-01 01:42:00')",
    "TreeNode{lhs=LeafNode{keyName='dt', operator='>=', value=2000-01-01 01:00:00.0}, andOr='AND', rhs=LeafNode{keyName='dt', operator='<=', value=2000-01-01 01:42:00.0}}");
  }

  @Test
  public void testBinaryExpressionWhenDateLiteralTypeIsNotSpecifiedNorQuoted() throws MetaException {
    checkFilter("(j = 1990-11-10 or j = 1990-11-11 and j = 1990-11-12)",
    "TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-10}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-11}, andOr='AND', rhs=LeafNode{keyName='j', operator='=', value=1990-11-12}}}");
  }

  @Test
  public void testBinaryExpressionWhenDateLiteralTypeIsSpecified() throws MetaException {
    checkFilter("(j = DATE'1990-11-10' or j = DATE'1990-11-11' and j = DATE'1990-11-12')",
    "TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-10}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-11}, andOr='AND', rhs=LeafNode{keyName='j', operator='=', value=1990-11-12}}}");
  }

  @Test
  public void testBinaryExpressionWhenTimeStampLiteralTypeIsNotSpecifiedNorQuoted() throws MetaException {
    checkFilter("(j = 1990-11-10 01:00:00 or j = 1990-11-11 01:00:24 and j = 1990-11-12 01:42:00)",
    "TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-10 01:00:00.0}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-11 01:00:24.0}, andOr='AND', rhs=LeafNode{keyName='j', operator='=', value=1990-11-12 01:42:00.0}}}");
  }

  @Test
  public void testBinaryExpressionWhenTimeStampLiteralTypeIsSpecified() throws MetaException {
    checkFilter("(j = TIMESTAMP'1990-11-10 01:00:00' or j = TIMESTAMP'1990-11-11 01:00:24' and j = TIMESTAMP'1990-11-12 01:42:00')",
    "TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-10 01:00:00.0}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-11 01:00:24.0}, andOr='AND', rhs=LeafNode{keyName='j', operator='=', value=1990-11-12 01:42:00.0}}}");
  }


  @Test
  public void testSingleColInExpressionWithIntLiteral() throws MetaException {
    checkFilter("(dt) IN (10, 20)",
    "TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=10}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=20}}");
  }

  @Test
  public void testBetweenExpressionWithIntLiteral() throws MetaException {
    checkFilter("dt between 10 and 20",
    "TreeNode{lhs=LeafNode{keyName='dt', operator='>=', value=10}, andOr='AND', rhs=LeafNode{keyName='dt', operator='<=', value=20}}");
  }

  @Test
  public void testBinaryExpressionWithIntLiteral() throws MetaException {
    checkFilter("dt = 10 or dt = 20",
    "TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=10}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=20}}");
  }

  @Test
  public void testSingleColInExpressionWithStringLiteral() throws MetaException {
    checkFilter("(dt) IN ('foo', 'bar')",
    "TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=foo}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=bar}}");
  }

  @Test
  public void testBinaryExpressionWithStringLiteral() throws MetaException {
    checkFilter("dt = 'foo' or dt = 'bar'",
    "TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=foo}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=bar}}");
  }

  @Test
  public void testSingleColInExpressionWithStringLikeDate() throws MetaException {
    checkFilter("(j) in ('1990-11-10', '1990-11-11', '1990-11-12')",
    "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-10}, andOr='OR', rhs=LeafNode{keyName='j', operator='=', value=1990-11-11}}, andOr='OR', rhs=LeafNode{keyName='j', operator='=', value=1990-11-12}}");
  }

  @Test
  public void testMultiColInExpressionWithDateLikeString() throws MetaException {
    checkFilter("(struct(ds1,ds2)) IN (struct('2000-05-08','2001-04-08'), struct('2000-05-09','2001-04-09'))",
    "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-08}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-08}}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-09}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-09}}}");
  }

  @Test
  public void testBetweenExpressionWithStringLikeDate() throws MetaException {
    checkFilter("(j BETWEEN '1990-11-10' AND '1990-11-11')",
    "TreeNode{lhs=LeafNode{keyName='j', operator='>=', value=1990-11-10}, andOr='AND', rhs=LeafNode{keyName='j', operator='<=', value=1990-11-11}}");
  }

  @Test
  public void testBinaryExpressionWithStringLikeDate() throws MetaException {
    checkFilter("(j = '1990-11-10' or j = '1990-11-11' and j = '1990-11-12')",
    "TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-10}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-11}, andOr='AND', rhs=LeafNode{keyName='j', operator='=', value=1990-11-12}}}");
  }

  @Test
  public void testSingleColInExpressionWithStringLikeTimestamp() throws MetaException {
    checkFilter("(dt) IN ('2000-01-01 01:00:00', '2000-01-01 01:42:00')",
    "TreeNode{lhs=LeafNode{keyName='dt', operator='=', value=2000-01-01 01:00:00}, andOr='OR', rhs=LeafNode{keyName='dt', operator='=', value=2000-01-01 01:42:00}}");
  }

  @Test
  public void testMultiColInExpressionWithTimestampLikeString() throws MetaException {
    checkFilter("(struct(ds1,ds2)) IN (struct('2000-05-08 01:00:00','2001-04-08 01:00:00'), struct('2000-05-09 01:00:00','2001-04-09 01:00:00'))",
    "TreeNode{lhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-08 01:00:00}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-08 01:00:00}}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='ds1', operator='=', value=2000-05-09 01:00:00}, andOr='AND', rhs=LeafNode{keyName='ds2', operator='=', value=2001-04-09 01:00:00}}}");
  }

  @Test
  public void testBetweenExpressionWithStringLikeTimestamp() throws MetaException {
    checkFilter("dt BETWEEN '2000-01-01 01:00:00' AND '2000-01-01 01:42:00')",
    "TreeNode{lhs=LeafNode{keyName='dt', operator='>=', value=2000-01-01 01:00:00}, andOr='AND', rhs=LeafNode{keyName='dt', operator='<=', value=2000-01-01 01:42:00}}");
  }

  @Test
  public void testBinaryExpressionWithStringLikeTimeStamp() throws MetaException {
    checkFilter("(j = '1990-11-10 01:00:00' or j = '1990-11-11 01:00:24' and j = '1990-11-12 01:42:00')",
    "TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-10 01:00:00}, andOr='OR', rhs=TreeNode{lhs=LeafNode{keyName='j', operator='=', value=1990-11-11 01:00:24}, andOr='AND', rhs=LeafNode{keyName='j', operator='=', value=1990-11-12 01:42:00}}}");
  }

  private void checkFilter(String filter, String expectTreeString) throws MetaException {
    ExpressionTree expressionTree = PartFilterExprUtil.parseFilterTree(filter);
    assertThat(expressionTree.getRoot().toString(), is(expectTreeString));
  }

  @Test
  public void testParseFilterWithInvalidDateWithType() {
    MetaException exception = assertThrows(MetaException.class,
            () -> PartFilterExprUtil.parseFilterTree("(j = DATE'2023-06-32')"));

    assertTrue(exception.getMessage().contains("Error parsing partition filter"));
  }

  @Test
  public void testParseFilterWithInvalidDateWithoutTypeNorQuoted() {
    MetaException exception = assertThrows(MetaException.class,
            () -> PartFilterExprUtil.parseFilterTree("(j = 2023-06-32)"));

    assertTrue(exception.getMessage().contains("Error parsing partition filter"));
  }

  @Test
  public void testParseFilterWithInvalidTimestampWithType() {
    MetaException exception = assertThrows(MetaException.class,
            () -> PartFilterExprUtil.parseFilterTree("(j = TIMESTAMP'2023-06-02 99:35:00')"));

    assertTrue(exception.getMessage().contains("Error parsing partition filter"));
  }

  @Test
  public void testParseFilterWithInvalidTimeStampWithoutTypeNorQuoted() {
    MetaException exception = assertThrows(MetaException.class,
            () -> PartFilterExprUtil.parseFilterTree("(j = 2023-06-02 99:35:00)"));

    assertTrue(exception.getMessage().contains("Error parsing partition filter"));
  }
}
