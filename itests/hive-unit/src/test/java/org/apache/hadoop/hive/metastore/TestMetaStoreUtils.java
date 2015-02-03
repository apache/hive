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

package org.apache.hadoop.hive.metastore;

import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

public class TestMetaStoreUtils extends TestCase {

  public void testTrimMapNullsXform() throws Exception {
    Map<String,String> m = new HashMap<String,String>();
    m.put("akey","aval");
    m.put("blank","");
    m.put("null",null);

    Map<String,String> xformed = MetaStoreUtils.trimMapNulls(m,true);
    assertEquals(3,xformed.size());
    assert(xformed.containsKey("akey"));
    assert(xformed.containsKey("blank"));
    assert(xformed.containsKey("null"));
    assertEquals("aval",xformed.get("akey"));
    assertEquals("",xformed.get("blank"));
    assertEquals("",xformed.get("null"));
  }

  public void testTrimMapNullsPrune() throws Exception {
    Map<String,String> m = new HashMap<String,String>();
    m.put("akey","aval");
    m.put("blank","");
    m.put("null",null);

    Map<String,String> pruned = MetaStoreUtils.trimMapNulls(m,false);
    assertEquals(2,pruned.size());
    assert(pruned.containsKey("akey"));
    assert(pruned.containsKey("blank"));
    assert(!pruned.containsKey("null"));
    assertEquals("aval",pruned.get("akey"));
    assertEquals("",pruned.get("blank"));
    assert(!pruned.containsValue(null));
  }



}
