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
package org.apache.hadoop.hive.ql.parse;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Parser tests for SQL11 Reserved KeyWords. Please find more information in
 * HIVE-6617. Total number : 83
 * ALL,ALTER,ARRAY,AS,AUTHORIZATION,BETWEEN,BIGINT,BINARY
 * ,BOOLEAN,BOTH,BY,CONSTRAINT
 * ,CREATE,CUBE,CURRENT_DATE,CURRENT_TIMESTAMP,CURSOR,
 * DATE,DECIMAL,DELETE,DESCRIBE
 * ,DOUBLE,DROP,EXISTS,EXTERNAL,FALSE,FETCH,FLOAT,FOR
 * ,FOREIGN,FULL,GRANT,GROUP,GROUPING
 * ,IMPORT,IN,INNER,INSERT,INT,INTERSECT,INTO,IS
 * ,LATERAL,LEFT,LIKE,LOCAL,MINUS,NONE,NULL
 * ,OF,ORDER,OUT,OUTER,PARTITION,PERCENT,PRECISION
 * ,PRIMARY,PROCEDURE,RANGE,READS,
 * REFERENCES,REGEXP,REVOKE,RIGHT,RLIKE,ROLLUP,ROW
 * ,ROWS,SET,SMALLINT,TABLE,TIMESTAMP
 * ,TO,TRIGGER,TRUE,TRUNCATE,UNION,UPDATE,USER,USING,VALUES,WITH,TIME
 */
public class TestSQL11ReservedKeyWordsNegative {
	private static HiveConf conf;

	private ParseDriver pd;

	@BeforeClass
	public static void initialize() {
		conf = new HiveConf(SemanticAnalyzer.class);
		SessionState.start(conf);
	}

	@Before
	public void setup() throws SemanticException, IOException {
		pd = new ParseDriver();
	}

	ASTNode parse(String query) throws ParseException {
		ASTNode nd = null;
		try {
			nd = pd.parse(query, new Context(conf));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (ASTNode) nd.getChild(0);
	}

	@Test
	public void testSQL11ReservedKeyWords_ALL() {
		try {
			parse("CREATE TABLE ALL (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'ALL' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_ALTER() {
		try {
			parse("CREATE TABLE ALTER (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'ALTER' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_ARRAY() {
		try {
			parse("CREATE TABLE ARRAY (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'ARRAY' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_AS() {
		try {
			parse("CREATE TABLE AS (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'AS' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_AUTHORIZATION() {
		try {
			parse("CREATE TABLE AUTHORIZATION (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'AUTHORIZATION' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_BETWEEN() {
		try {
			parse("CREATE TABLE BETWEEN (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'BETWEEN' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_BIGINT() {
		try {
			parse("CREATE TABLE BIGINT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'BIGINT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_BINARY() {
		try {
			parse("CREATE TABLE BINARY (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'BINARY' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_BOOLEAN() {
		try {
			parse("CREATE TABLE BOOLEAN (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'BOOLEAN' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_BOTH() {
		try {
			parse("CREATE TABLE BOTH (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'BOTH' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_BY() {
		try {
			parse("CREATE TABLE BY (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'BY' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_CONSTRAINT() {
		try {
			parse("CREATE TABLE CONSTRAINT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'CONSTRAINT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_CREATE() {
		try {
			parse("CREATE TABLE CREATE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'CREATE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_CUBE() {
		try {
			parse("CREATE TABLE CUBE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'CUBE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_CURRENT_DATE() {
		try {
			parse("CREATE TABLE CURRENT_DATE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'CURRENT_DATE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_CURRENT_TIMESTAMP() {
		try {
			parse("CREATE TABLE CURRENT_TIMESTAMP (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'CURRENT_TIMESTAMP' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_CURSOR() {
		try {
			parse("CREATE TABLE CURSOR (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'CURSOR' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_DATE() {
		try {
			parse("CREATE TABLE DATE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'DATE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_DECIMAL() {
		try {
			parse("CREATE TABLE DECIMAL (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'DECIMAL' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_DELETE() {
		try {
			parse("CREATE TABLE DELETE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'DELETE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_DESCRIBE() {
		try {
			parse("CREATE TABLE DESCRIBE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'DESCRIBE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_DOUBLE() {
		try {
			parse("CREATE TABLE DOUBLE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'DOUBLE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_DROP() {
		try {
			parse("CREATE TABLE DROP (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'DROP' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_EXISTS() {
		try {
			parse("CREATE TABLE EXISTS (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'EXISTS' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_EXTERNAL() {
		try {
			parse("CREATE TABLE EXTERNAL (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'EXTERNAL' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_FALSE() {
		try {
			parse("CREATE TABLE FALSE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'FALSE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_FETCH() {
		try {
			parse("CREATE TABLE FETCH (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'FETCH' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_FLOAT() {
		try {
			parse("CREATE TABLE FLOAT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'FLOAT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_FOR() {
		try {
			parse("CREATE TABLE FOR (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'FOR' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_FOREIGN() {
		try {
			parse("CREATE TABLE FOREIGN (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'FOREIGN' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_FULL() {
		try {
			parse("CREATE TABLE FULL (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'FULL' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_GRANT() {
		try {
			parse("CREATE TABLE GRANT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'GRANT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_GROUP() {
		try {
			parse("CREATE TABLE GROUP (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'GROUP' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_GROUPING() {
		try {
			parse("CREATE TABLE GROUPING (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'GROUPING' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_IMPORT() {
		try {
			parse("CREATE TABLE IMPORT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'IMPORT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_IN() {
		try {
			parse("CREATE TABLE IN (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'IN' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_INNER() {
		try {
			parse("CREATE TABLE INNER (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'INNER' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_INSERT() {
		try {
			parse("CREATE TABLE INSERT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'INSERT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_INT() {
		try {
			parse("CREATE TABLE INT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'INT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_INTERSECT() {
		try {
			parse("CREATE TABLE INTERSECT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'INTERSECT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_INTO() {
		try {
			parse("CREATE TABLE INTO (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'INTO' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_IS() {
		try {
			parse("CREATE TABLE IS (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'IS' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_LATERAL() {
		try {
			parse("CREATE TABLE LATERAL (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'LATERAL' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_LEFT() {
		try {
			parse("CREATE TABLE LEFT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'LEFT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_LIKE() {
		try {
			parse("CREATE TABLE LIKE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'LIKE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_LOCAL() {
		try {
			parse("CREATE TABLE LOCAL (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'LOCAL' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_MINUS() {
		try {
			parse("CREATE TABLE MINUS (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'MINUS' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_NONE() {
	  try {
	    parse("CREATE TABLE NONE (col STRING)");
	    Assert.assertFalse("Expected ParseException", true);
	  } catch (ParseException ex) {
	    Assert.assertEquals(
	        "Failure didn't match.",
	        "line 1:13 cannot recognize input near 'NONE' '(' 'col' in table name",
	        ex.getMessage());
	  }
	}

	@Test
	public void testSQL11ReservedKeyWords_NULL() {
		try {
			parse("CREATE TABLE NULL (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'NULL' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_OF() {
		try {
			parse("CREATE TABLE OF (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'OF' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_ORDER() {
		try {
			parse("CREATE TABLE ORDER (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'ORDER' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_OUT() {
		try {
			parse("CREATE TABLE OUT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'OUT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_OUTER() {
		try {
			parse("CREATE TABLE OUTER (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'OUTER' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_PARTITION() {
		try {
			parse("CREATE TABLE PARTITION (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'PARTITION' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_PERCENT() {
		try {
			parse("CREATE TABLE PERCENT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'PERCENT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_PRECISION() {
		try {
			parse("CREATE TABLE PRECISION (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'PRECISION' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_PRIMARY() {
		try {
			parse("CREATE TABLE PRIMARY (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'PRIMARY' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_PROCEDURE() {
		try {
			parse("CREATE TABLE PROCEDURE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'PROCEDURE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_RANGE() {
		try {
			parse("CREATE TABLE RANGE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'RANGE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_READS() {
		try {
			parse("CREATE TABLE READS (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'READS' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_REFERENCES() {
		try {
			parse("CREATE TABLE REFERENCES (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'REFERENCES' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_REGEXP() {
		try {
			parse("CREATE TABLE REGEXP (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'REGEXP' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_REVOKE() {
		try {
			parse("CREATE TABLE REVOKE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'REVOKE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_RIGHT() {
		try {
			parse("CREATE TABLE RIGHT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'RIGHT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_RLIKE() {
		try {
			parse("CREATE TABLE RLIKE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'RLIKE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_ROLLUP() {
		try {
			parse("CREATE TABLE ROLLUP (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'ROLLUP' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_ROW() {
		try {
			parse("CREATE TABLE ROW (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'ROW' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_ROWS() {
		try {
			parse("CREATE TABLE ROWS (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'ROWS' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_SET() {
		try {
			parse("CREATE TABLE SET (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'SET' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_SMALLINT() {
		try {
			parse("CREATE TABLE SMALLINT (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'SMALLINT' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_TABLE() {
		try {
			parse("CREATE TABLE TABLE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'TABLE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_TIMESTAMP() {
		try {
			parse("CREATE TABLE TIMESTAMP (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'TIMESTAMP' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_TO() {
		try {
			parse("CREATE TABLE TO (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'TO' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_TRIGGER() {
		try {
			parse("CREATE TABLE TRIGGER (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'TRIGGER' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_TRUE() {
		try {
			parse("CREATE TABLE TRUE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'TRUE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_TRUNCATE() {
		try {
			parse("CREATE TABLE TRUNCATE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'TRUNCATE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_UNION() {
		try {
			parse("CREATE TABLE UNION (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'UNION' '(' 'col' in table name",
					ex.getMessage());
		}
	}

  @Test
  public void testSQL11ReservedKeyWords_UNIQUE() {
    try {
      parse("CREATE TABLE UNIQUE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals(
          "Failure didn't match.",
          "line 1:13 cannot recognize input near 'UNIQUE' '(' 'col' in table name",
          ex.getMessage());
    }
  }

	@Test
	public void testSQL11ReservedKeyWords_UPDATE() {
		try {
			parse("CREATE TABLE UPDATE (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'UPDATE' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_USER() {
		try {
			parse("CREATE TABLE USER (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'USER' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_USING() {
		try {
			parse("CREATE TABLE USING (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'USING' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_VALUES() {
		try {
			parse("CREATE TABLE VALUES (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'VALUES' '(' 'col' in table name",
					ex.getMessage());
		}
	}

	@Test
	public void testSQL11ReservedKeyWords_WITH() {
		try {
			parse("CREATE TABLE WITH (col STRING)");
			Assert.assertFalse("Expected ParseException", true);
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:13 cannot recognize input near 'WITH' '(' 'col' in table name",
					ex.getMessage());
		}
	}

  @Test
  public void testSQL11ReservedKeyWords_TIME() {
    try {
      parse("CREATE TABLE TIME (col STRING)");
      Assert.fail("Expected ParseException");
    } catch (ParseException ex) {
      Assert.assertEquals(
          "Failure didn't match.",
          "line 1:13 cannot recognize input near 'TIME' '(' 'col' in table name",
          ex.getMessage());
    }
  }

	@Test
	public void testSQL11ReservedKeyWords_KILL() {
		try {
			parse("CREATE TABLE KILL QUERY (col STRING)");
			Assert.fail("Expected ParseException");
		} catch (ParseException ex) {
			Assert.assertEquals(
					"Failure didn't match.",
					"line 1:18 cannot recognize input near 'QUERY' '(' 'col' in create table statement",
					ex.getMessage());
		}
	}

}
