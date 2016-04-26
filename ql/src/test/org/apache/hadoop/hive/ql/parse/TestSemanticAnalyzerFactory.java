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
package org.apache.hadoop.hive.ql.parse;

import junit.framework.Assert;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.junit.Before;
import org.junit.Test;

public class TestSemanticAnalyzerFactory {

  private QueryState queryState;
  private HiveConf conf;
  
  @Before
  public void setup() throws Exception {
    queryState = new QueryState(null);
    conf = queryState.getConf();
  }
  @Test
  public void testCreate() throws Exception {
    BaseSemanticAnalyzer analyzer = SemanticAnalyzerFactory.
        get(queryState, new ASTNode(new CommonToken(HiveParser.TOK_CREATEMACRO)));
    Assert.assertTrue(analyzer.getClass().getSimpleName(), analyzer instanceof MacroSemanticAnalyzer);
  }
  @Test
  public void testDrop() throws Exception {
    BaseSemanticAnalyzer analyzer = SemanticAnalyzerFactory.
        get(queryState, new ASTNode(new CommonToken(HiveParser.TOK_DROPMACRO)));
    Assert.assertTrue(analyzer.getClass().getSimpleName(), analyzer instanceof MacroSemanticAnalyzer);
  }
}