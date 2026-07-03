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
 */
package org.apache.hadoop.hive.ql.parse;

import org.junit.Assert;
import org.junit.Test;

public class TestParseCreateTable {
  private final ParseDriver parseDriver = new ParseDriver();

  @Test
  public void testParseCreateTable() throws Exception {
    ASTNode tree = parseDriver.parse(
        "CREATE TABLE IF NOT EXISTS foo (m INT, b STRING) STORED AS ORC").getTree();
    final String dump = tree.toStringTree();
    System.out.println("AST: " + dump);
    Assert.assertTrue("CREATE TABLE should produce tok_createtable, got: " + dump,
        dump.contains("tok_createtable"));
    Assert.assertFalse("CREATE TABLE must NOT produce tok_createrole, got: " + dump,
        dump.contains("tok_createrole"));
  }
}
