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

package org.apache.hive.service.cli;

import junit.framework.Assert;
import org.junit.Test;

public class TestHiveSQLException {

  @Test
  public void testExceptionMarshalling() throws Exception {
    Exception ex1 = ex1();
    ex1.initCause(ex2());
    Throwable ex = HiveSQLException.toCause(HiveSQLException.toString(ex1));
    Assert.assertSame(RuntimeException.class, ex.getClass());
    Assert.assertEquals("exception1", ex.getMessage());
    Assert.assertSame(UnsupportedOperationException.class, ex.getCause().getClass());
    Assert.assertEquals("exception2", ex.getCause().getMessage());
  }

  private static Exception ex1() {
    return new RuntimeException("exception1");
  }

  private static Exception ex2() {
    return new UnsupportedOperationException("exception2");
  }
}
