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

package org.apache.hadoop.hive.ql.udf;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.junit.Test;

public class TestUDFUUID extends TestCase {
  @Test
  public void testUUID() throws Exception {
    UDFUUID udf = new UDFUUID();
    
    String id1 = udf.evaluate().toString();
    String id2 = udf.evaluate().toString();
    
    assertFalse(id1.equals(id2));
    
    assertEquals(id1.length(), 36);
    assertEquals(id2.length(), 36);

    GenericUDFBridge bridge = new GenericUDFBridge("uuid", false, UDFUUID.class.getName());
    assertFalse(FunctionRegistry.isDeterministic(bridge));
  }
}
