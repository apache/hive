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

package org.apache.hadoop.hive.ql;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.hive.common.JavaUtils;
import org.junit.Test;

public class TestErrorMsg {

  @Test
  public void testUniqueErrorCode() {
    Set<Integer> numbers = new HashSet<Integer>();
    for (ErrorMsg err : ErrorMsg.values()) {
      int code = err.getErrorCode();
      Assert.assertTrue("duplicated error number " + code, numbers.add(code));
    }
  }
  @Test
  public void testReverseMatch() {
    testReverseMatch(ErrorMsg.OP_NOT_ALLOWED_IN_IMPLICIT_TXN, "COMMIT");
    testReverseMatch(ErrorMsg.OP_NOT_ALLOWED_IN_TXN, "ALTER TABLE",
      JavaUtils.txnIdToString(1), "123");
    testReverseMatch(ErrorMsg.OP_NOT_ALLOWED_WITHOUT_TXN, "ROLLBACK");
  }
  private void testReverseMatch(ErrorMsg errorMsg, String... args) {
    String parametrizedMsg = errorMsg.format(args);
    ErrorMsg canonicalMsg = ErrorMsg.getErrorMsg(parametrizedMsg);
    Assert.assertEquals("Didn't find expected msg", errorMsg.getErrorCode(), canonicalMsg.getErrorCode());
  }
}
