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
package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link TezDummyStoreOperator#closeOp(boolean)}.
 *
 * When a Tez container is reused for a subsequent task of the same reducer vertex, Hive's
 * per-query ObjectCache (keyed by vertex name) can hand the new task the same already-
 * initialized DummyStore/MergeJoin operator instances the previous task used. If closeOp()
 * leaves stale wiring behind (this operator still listed as a parent of its former child,
 * and/or that child still referenced from childOperators), the reused instances carry over
 * incorrect state into the next task's execution, causing
 * ReduceRecordProcessor.getJoinParentOp() to walk into the wrong operator and throw
 * IllegalStateException("Was expecting dummy store operator but found: ...").
 */
public class TestTezDummyStoreOperator {

  private TezDummyStoreOperator dummyStore;

  @Before
  public void setUp() {
    dummyStore = new TezDummyStoreOperator();
  }

  @Test
  public void testCloseOpRemovesSelfFromChildParents() throws HiveException {
    CommonMergeJoinOperator mergeJoin = new CommonMergeJoinOperator();

    wireParentChild(dummyStore, mergeJoin);

    Assert.assertTrue("precondition: mergeJoin's parents must contain dummyStore",
        mergeJoin.getParentOperators().contains(dummyStore));
    Assert.assertTrue("precondition: dummyStore's children must contain mergeJoin",
        dummyStore.getChildOperators().contains(mergeJoin));

    dummyStore.closeOp(false);

    Assert.assertFalse("dummyStore must be removed from its former child's parentOperators",
        mergeJoin.getParentOperators().contains(dummyStore));
    Assert.assertTrue("dummyStore's own childOperators must be cleared",
        dummyStore.getChildOperators().isEmpty());
  }

  @Test
  public void testCloseOpRemovesSelfFromAllChildrenWhenMultiple() throws HiveException {
    CommonMergeJoinOperator child1 = new CommonMergeJoinOperator();
    CommonMergeJoinOperator child2 = new CommonMergeJoinOperator();

    List<Operator<? extends OperatorDesc>> children = new ArrayList<>();
    children.add(child1);
    children.add(child2);
    dummyStore.setChildOperators(children);

    List<Operator<? extends OperatorDesc>> parentsOfChild1 = new ArrayList<>();
    parentsOfChild1.add(dummyStore);
    child1.setParentOperators(parentsOfChild1);

    List<Operator<? extends OperatorDesc>> parentsOfChild2 = new ArrayList<>();
    parentsOfChild2.add(dummyStore);
    child2.setParentOperators(parentsOfChild2);

    dummyStore.closeOp(false);

    Assert.assertFalse(child1.getParentOperators().contains(dummyStore));
    Assert.assertFalse(child2.getParentOperators().contains(dummyStore));
    Assert.assertTrue(dummyStore.getChildOperators().isEmpty());
  }

  @Test
  public void testCloseOpWithNoChildrenDoesNotThrow() throws HiveException {
    dummyStore.setChildOperators(new ArrayList<>());
    dummyStore.closeOp(false);
    Assert.assertTrue(dummyStore.getChildOperators().isEmpty());
  }

  @Test
  public void testCloseOpWithNullChildrenDoesNotThrow() throws HiveException {
    dummyStore.setChildOperators(null);
    // setChildOperators(null) replaces null with an empty list (see Operator#setChildOperators).
    dummyStore.closeOp(false);
    Assert.assertTrue(dummyStore.getChildOperators().isEmpty());
  }

  @Test
  public void testCloseOpDoesNotAffectUnrelatedParentReferences() throws HiveException {
    CommonMergeJoinOperator mergeJoin = new CommonMergeJoinOperator();
    TezDummyStoreOperator otherDummyStore = new TezDummyStoreOperator();

    List<Operator<? extends OperatorDesc>> parents = new ArrayList<>();
    parents.add(otherDummyStore);
    parents.add(dummyStore);
    mergeJoin.setParentOperators(parents);

    List<Operator<? extends OperatorDesc>> children = new ArrayList<>();
    children.add(mergeJoin);
    dummyStore.setChildOperators(children);

    dummyStore.closeOp(false);

    Assert.assertFalse(mergeJoin.getParentOperators().contains(dummyStore));
    Assert.assertTrue("unrelated parent reference must be left untouched",
        mergeJoin.getParentOperators().contains(otherDummyStore));
  }

  private static void wireParentChild(
      Operator<? extends OperatorDesc> parent, Operator<? extends OperatorDesc> child) {
    List<Operator<? extends OperatorDesc>> children = new ArrayList<>();
    children.add(child);
    parent.setChildOperators(children);

    List<Operator<? extends OperatorDesc>> parents = new ArrayList<>();
    parents.add(parent);
    child.setParentOperators(parents);
  }
}
