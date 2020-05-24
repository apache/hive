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
package org.apache.hadoop.hive.ql.io.filter;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test creation and manipulation of MutableFilterContext and FilterContext.
 */
public class TestFilterContext {

  private int[] makeValidSelected() {
    int[] selected = new int[512];
    for (int i=0; i < selected.length; i++){
      selected[i] = i*2;
    }
    return selected;
  }

  private int[] makeInvalidSelected() {
    int[] selected = new int[512];
    Arrays.fill(selected, 1);
    return selected;
  }

  @Test
  public void testInitFilterContext(){
    MutableFilterContext mutableFilterContext = new VectorizedRowBatch(0);
    int[] selected = makeValidSelected();

    mutableFilterContext.setFilterContext(true, selected, selected.length);
    FilterContext filterContext = mutableFilterContext.immutable();

    Assert.assertEquals(true, filterContext.isSelectedInUse());
    Assert.assertEquals(512, filterContext.getSelectedSize());
    Assert.assertEquals(512, filterContext.getSelected().length);
  }


  @Test
  public void testResetFilterContext(){
    MutableFilterContext mutableFilterContext = new VectorizedRowBatch(0);
    int[] selected = makeValidSelected();

    mutableFilterContext.setFilterContext(true, selected, selected.length);
    FilterContext filterContext = mutableFilterContext.immutable();

    Assert.assertEquals(true, filterContext.isSelectedInUse());
    Assert.assertEquals(512, filterContext.getSelectedSize());
    Assert.assertEquals(512, filterContext.getSelected().length);

    filterContext.reset();

    Assert.assertEquals(false, filterContext.isSelectedInUse());
    Assert.assertEquals(0, filterContext.getSelectedSize());
  }

  @Test(expected=AssertionError.class)
  public void testInitInvalidFilterContext(){
    MutableFilterContext mutableFilterContext = new VectorizedRowBatch(0);
    int[] selected = makeInvalidSelected();

    mutableFilterContext.setFilterContext(true, selected, selected.length);
  }

  @Test
  public void testBorrowSelected(){
    MutableFilterContext mutableFilterContext = new VectorizedRowBatch(0);
    mutableFilterContext.setFilterContext(true, new int[] {100, 200}, 2);

    int[] borrowedSelected = mutableFilterContext.updateSelected(1);
    // make sure we borrowed the existing array
    Assert.assertEquals(2, borrowedSelected.length);
    Assert.assertEquals(100, borrowedSelected[0]);
    Assert.assertEquals(200, borrowedSelected[1]);

    borrowedSelected = mutableFilterContext.updateSelected(3);
    Assert.assertEquals(3, borrowedSelected.length);
    Assert.assertEquals(0, borrowedSelected[0]);
    Assert.assertEquals(0, borrowedSelected[1]);
    Assert.assertEquals(0, borrowedSelected[2]);
  }
}
