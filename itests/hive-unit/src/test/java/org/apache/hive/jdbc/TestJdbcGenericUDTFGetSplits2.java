/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import org.junit.Test;

/**
 * TestJdbcGenericUDTFGetSplits2.
 */
public class TestJdbcGenericUDTFGetSplits2 extends AbstractTestJdbcGenericUDTFGetSplits {

  @Test(timeout = 200000)
  public void testGenericUDTFOrderBySplitCount1() throws Exception {
    super.testGenericUDTFOrderBySplitCount1("get_llap_splits", new int[]{12, 3, 1, 3, 12});
  }

}
