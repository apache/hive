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

package org.apache.hadoop.hive.ql.optimizer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;

import org.junit.Test;

public class TestConvertJoinMapJoin {

  @Test
  public void testHasOuterJoin1() throws SemanticException {
    JoinCondDesc[] condDesc = new JoinCondDesc[2];
    condDesc[0] = new JoinCondDesc(0, 1, JoinDesc.INNER_JOIN);
    condDesc[1] = new JoinCondDesc(1, 2, JoinDesc.LEFT_OUTER_JOIN);
    JoinDesc joinDesc = new JoinDesc();
    joinDesc.setConds(condDesc);
    CompilationOpContext cCtx = new CompilationOpContext();
    JoinOperator joinOperator = (JoinOperator) OperatorFactory.get(cCtx, joinDesc);
    assertTrue(ConvertJoinMapJoin.hasOuterJoin(joinOperator));
  }

  @Test
  public void testHasOuterJoin2() throws SemanticException {
    JoinCondDesc[] condDesc = new JoinCondDesc[2];
    condDesc[0] = new JoinCondDesc(0, 1, JoinDesc.LEFT_OUTER_JOIN);
    condDesc[1] = new JoinCondDesc(1, 2, JoinDesc.INNER_JOIN);
    JoinDesc joinDesc = new JoinDesc();
    joinDesc.setConds(condDesc);
    CompilationOpContext cCtx = new CompilationOpContext();
    JoinOperator joinOperator = (JoinOperator) OperatorFactory.get(cCtx, joinDesc);
    assertTrue(ConvertJoinMapJoin.hasOuterJoin(joinOperator));
  }

  @Test
  public void testHasOuterJoin3() throws SemanticException {
    JoinCondDesc[] condDesc = new JoinCondDesc[2];
    condDesc[0] = new JoinCondDesc(0, 1, JoinDesc.INNER_JOIN);
    condDesc[1] = new JoinCondDesc(1, 2, JoinDesc.INNER_JOIN);
    JoinDesc joinDesc = new JoinDesc();
    joinDesc.setConds(condDesc);
    CompilationOpContext cCtx = new CompilationOpContext();
    JoinOperator joinOperator = (JoinOperator) OperatorFactory.get(cCtx, joinDesc);
    assertFalse(ConvertJoinMapJoin.hasOuterJoin(joinOperator));
  }
}
