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

package org.apache.hadoop.hive.serde2.typeinfo;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for BaseCharTypes.
 */
public class TestBaseCharTypeInfo {

  /**
   * Test that VARCHAR and CHAR are not considered to be the same.
   */
  @Test
  public void testVarCharToCharEqualityDefault() {
    VarcharTypeInfo vcharTypeInfo = new VarcharTypeInfo();
    CharTypeInfo charTypeInfo = new CharTypeInfo();

    Assert.assertNotEquals(vcharTypeInfo, charTypeInfo);
  }

  /**
   * Test that VARCHAR(10) and CHAR(10) are not considered to be the same.
   */
  @Test
  public void testVarCharToCharEqualitySize() {
    VarcharTypeInfo vcharTypeInfo = new VarcharTypeInfo(10);
    CharTypeInfo charTypeInfo = new CharTypeInfo(10);

    Assert.assertNotEquals(vcharTypeInfo, charTypeInfo);
  }
}
