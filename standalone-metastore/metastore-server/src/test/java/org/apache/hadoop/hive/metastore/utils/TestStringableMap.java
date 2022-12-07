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
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

@Category(MetastoreUnitTest.class)
public class TestStringableMap {
    @Test
    public void stringableMap() throws Exception {
        // Empty map case
        StringableMap m = new StringableMap(new HashMap<String, String>());
        String s = m.toString();
        Assert.assertEquals("0:", s);
        m = new StringableMap(s);
        Assert.assertEquals(0, m.size());

        Map<String, String> base = new HashMap<String, String>();
        base.put("mary", "poppins");
        base.put("bert", null);
        base.put(null, "banks");
        m = new StringableMap(base);
        s = m.toString();
        m = new StringableMap(s);
        Assert.assertEquals(3, m.size());
        Map<String, Boolean> saw = new HashMap<String, Boolean>(3);
        saw.put("mary", false);
        saw.put("bert", false);
        saw.put(null, false);
        for (Map.Entry<String, String> e : m.entrySet()) {
            saw.put(e.getKey(), true);
            if ("mary".equals(e.getKey())) Assert.assertEquals("poppins", e.getValue());
            else if ("bert".equals(e.getKey())) Assert.assertNull(e.getValue());
            else if (null == e.getKey()) Assert.assertEquals("banks", e.getValue());
            else Assert.fail("Unexpected value " + e.getKey());
        }
        Assert.assertEquals(3, saw.size());
        Assert.assertTrue(saw.get("mary"));
        Assert.assertTrue(saw.get("bert"));
        Assert.assertTrue(saw.get(null));
    }
}