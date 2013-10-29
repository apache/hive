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
package org.apache.hadoop.hive.ql.optimizer.listbucketingpruner;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.Test;

/**
 *
 * Test {@link ListBucketingPruner}
 *
 */
public class TestListBucketingPrunner extends TestCase {

  @Test
  public void testSkipSkewedDirectory1() {
    Assert.assertFalse(ListBucketingPrunerUtils.skipSkewedDirectory(null)) ;
  }

  @Test
  public void testSkipSkewedDirectory2() {
    Assert.assertTrue(ListBucketingPrunerUtils.skipSkewedDirectory(Boolean.FALSE)) ;
  }

  @Test
  public void testSkipSkewedDirectory3() {
    Assert.assertFalse(ListBucketingPrunerUtils.skipSkewedDirectory(Boolean.TRUE)) ;
  }

  @Test
  public void testAndBoolOperand() {
    /**
     * Operand one|Operand another | And result
     */
    // unknown | T | unknown
    Assert.assertNull(ListBucketingPrunerUtils.andBoolOperand(null, Boolean.TRUE));
    // unknown | F | F
    Assert.assertFalse(ListBucketingPrunerUtils.andBoolOperand(null, Boolean.FALSE));
    // unknown | unknown | unknown
    Assert.assertNull(ListBucketingPrunerUtils.andBoolOperand(null, null));
    // T | T | T
    Assert.assertTrue(ListBucketingPrunerUtils.andBoolOperand(Boolean.TRUE, Boolean.TRUE));
    // T | F | F
    Assert.assertFalse(ListBucketingPrunerUtils.andBoolOperand(Boolean.TRUE, Boolean.FALSE));
    // T | unknown | unknown
    Assert.assertNull(ListBucketingPrunerUtils.andBoolOperand(Boolean.TRUE, null));
    // F | T | F
    Assert.assertFalse(ListBucketingPrunerUtils.andBoolOperand(Boolean.FALSE, Boolean.TRUE));
    // F | F | F
    Assert.assertFalse(ListBucketingPrunerUtils.andBoolOperand(Boolean.FALSE, Boolean.FALSE));
    // F | unknown | F
    Assert.assertFalse(ListBucketingPrunerUtils.andBoolOperand(Boolean.FALSE, null));
  }

  @Test
  public void testOrBoolOperand() {
    // Operand one|Operand another | or result
    // unknown | T | T
    Assert.assertTrue(ListBucketingPrunerUtils.orBoolOperand(null, Boolean.TRUE));
    // unknown | F | unknown
    Assert.assertNull(ListBucketingPrunerUtils.orBoolOperand(null, Boolean.FALSE));
    // unknown | unknown | unknown
    Assert.assertNull(ListBucketingPrunerUtils.orBoolOperand(null, Boolean.FALSE));
    // T | T | T
    Assert.assertTrue(ListBucketingPrunerUtils.orBoolOperand(Boolean.TRUE, Boolean.TRUE));
    // T | F | T
    Assert.assertTrue(ListBucketingPrunerUtils.orBoolOperand(Boolean.TRUE, Boolean.FALSE));
    // T | unknown | unknown
    Assert.assertNull(ListBucketingPrunerUtils.orBoolOperand(Boolean.TRUE, null));
    // F | T | T
    Assert.assertTrue(ListBucketingPrunerUtils.orBoolOperand(Boolean.FALSE, Boolean.TRUE));
    // F | F | F
    Assert.assertFalse(ListBucketingPrunerUtils.orBoolOperand(Boolean.FALSE, Boolean.FALSE));
    // F | unknown | unknown
    Assert.assertNull(ListBucketingPrunerUtils.orBoolOperand(Boolean.FALSE, null));
  }

  @Test
  public void testNotBoolOperand() {
    // Operand | Not
    // T | F
    Assert.assertFalse(ListBucketingPrunerUtils.notBoolOperand(Boolean.TRUE));
    // F | T
    Assert.assertTrue(ListBucketingPrunerUtils.notBoolOperand(Boolean.FALSE));
    // unknown | unknown
    Assert.assertNull(ListBucketingPrunerUtils.notBoolOperand(null));
  }
}
