/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;



public class TestStringableList {

  @Test
  public void stringableList() throws Exception {
    // Empty list case
    MRCompactor.StringableList ls = new MRCompactor.StringableList();
    String s = ls.toString();
    Assert.assertEquals("0:", s);
    ls = new MRCompactor.StringableList(s);
    Assert.assertEquals(0, ls.size());

    ls = new MRCompactor.StringableList();
    ls.add(new Path("/tmp"));
    ls.add(new Path("/usr"));
    s = ls.toString();
    Assert.assertTrue("Expected 2:4:/tmp4:/usr or 2:4:/usr4:/tmp, got " + s,
        "2:4:/tmp4:/usr".equals(s) || "2:4:/usr4:/tmp".equals(s));
    ls = new MRCompactor.StringableList(s);
    Assert.assertEquals(2, ls.size());
    boolean sawTmp = false, sawUsr = false;
    for (Path p : ls) {
      if ("/tmp".equals(p.toString())) sawTmp = true;
      else if ("/usr".equals(p.toString())) sawUsr = true;
      else Assert.fail("Unexpected path " + p.toString());
    }
    Assert.assertTrue(sawTmp);
    Assert.assertTrue(sawUsr);
  }
}