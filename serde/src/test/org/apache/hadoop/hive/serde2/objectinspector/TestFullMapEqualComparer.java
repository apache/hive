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
package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;

public class TestFullMapEqualComparer extends TestCase {

  public static class IntegerIntegerMapHolder {
    Map<Integer, Integer> mMap;

    public IntegerIntegerMapHolder() {
      mMap = new TreeMap<Integer, Integer>();
    }
  }

  public void testAntiSymmetry() {
    IntegerIntegerMapHolder o1 = new IntegerIntegerMapHolder();
    IntegerIntegerMapHolder o2 = new IntegerIntegerMapHolder();

    ObjectInspector oi = ObjectInspectorFactory
        .getReflectionObjectInspector(IntegerIntegerMapHolder.class, ObjectInspectorOptions.JAVA);

    o1.mMap.put(1, 1);

    o2.mMap.put(0, 99);

    { // not anti-symmetric
      int rc12 = ObjectInspectorUtils.compare(o1, oi, o2, oi, new SimpleMapEqualComparer());
      assertTrue(rc12 > 0);
      int rc21 = ObjectInspectorUtils.compare(o2, oi, o1, oi, new SimpleMapEqualComparer());
      assertTrue(rc21 > 0);
    }
    { // not anti-symmetric
      int rc12 = ObjectInspectorUtils.compare(o1, oi, o2, oi, new CrossMapEqualComparer());
      assertTrue(rc12 > 0);
      int rc21 = ObjectInspectorUtils.compare(o2, oi, o1, oi, new CrossMapEqualComparer());
      assertTrue(rc21 > 0);
    }
    {// anti-symmetric
      int rc12 = ObjectInspectorUtils.compare(o1, oi, o2, oi, new FullMapEqualComparer());
      assertTrue(rc12 > 0);
      int rc21 = ObjectInspectorUtils.compare(o2, oi, o1, oi, new FullMapEqualComparer());
      assertTrue(rc21 < 0);
    }

  }

  public void testTransitivity() {
    IntegerIntegerMapHolder o1 = new IntegerIntegerMapHolder();
    IntegerIntegerMapHolder o2 = new IntegerIntegerMapHolder();
    IntegerIntegerMapHolder o3 = new IntegerIntegerMapHolder();

    ObjectInspector oi = ObjectInspectorFactory
        .getReflectionObjectInspector(IntegerIntegerMapHolder.class, ObjectInspectorOptions.JAVA);

    o1.mMap.put(1, 1);
    o1.mMap.put(99, 99);

    o2.mMap.put(0, 99);
    o2.mMap.put(99, 99);

    o3.mMap.put(0, 1);
    o3.mMap.put(1, 99);

    { // non-transitive
      int rc12 = ObjectInspectorUtils.compare(o1, oi, o2, oi, new SimpleMapEqualComparer());
      assertTrue(rc12 > 0);
      int rc23 = ObjectInspectorUtils.compare(o2, oi, o3, oi, new SimpleMapEqualComparer());
      assertTrue(rc23 > 0);
      int rc13 = ObjectInspectorUtils.compare(o1, oi, o3, oi, new SimpleMapEqualComparer());
      assertTrue(rc13 < 0);
    }
    { // non-transitive
      int rc12 = ObjectInspectorUtils.compare(o1, oi, o2, oi, new CrossMapEqualComparer());
      assertTrue(rc12 > 0);
      int rc23 = ObjectInspectorUtils.compare(o2, oi, o3, oi, new CrossMapEqualComparer());
      assertTrue(rc23 > 0);
      int rc13 = ObjectInspectorUtils.compare(o1, oi, o3, oi, new CrossMapEqualComparer());
      assertTrue(rc13 < 0);
    }
    {// transitive
      int rc12 = ObjectInspectorUtils.compare(o1, oi, o2, oi, new FullMapEqualComparer());
      assertTrue(rc12 > 0);
      int rc23 = ObjectInspectorUtils.compare(o2, oi, o3, oi, new FullMapEqualComparer());
      assertTrue(rc23 > 0);
      int rc13 = ObjectInspectorUtils.compare(o1, oi, o3, oi, new FullMapEqualComparer());
      assertTrue(rc13 > 0);
    }
  }

}
