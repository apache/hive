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

package org.apache.hadoop.hive.ql.exec;

import java.util.HashMap;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * TestHashMapWrapper.
 *
 */
public class TestHashMapWrapper extends TestCase {

  public void testHashMapWrapper() throws Exception {

    HashMap<String, String> mem_map = new HashMap<String, String>();
    mem_map.put("k1", "v1");
    mem_map.put("k2", "v2");
    mem_map.put("k3", "v3");
    mem_map.put("k4", "v4");

    try {
      // NO cache
      HashMapWrapper<String, String> wrapper = new HashMapWrapper<String, String>(0);
      insertAll(wrapper, mem_map);
      checkAll(wrapper, mem_map);
      wrapper.close(); // clean up temporary files

      // cache size = 1
      wrapper = new HashMapWrapper<String, String>(1);
      insertAll(wrapper, mem_map);
      checkAll(wrapper, mem_map);
      wrapper.close(); // clean up temporary files

      // cache size = 2
      wrapper = new HashMapWrapper<String, String>(2);
      insertAll(wrapper, mem_map);
      checkAll(wrapper, mem_map);
      wrapper.close(); // clean up temporary files

      // cache size = 4
      wrapper = new HashMapWrapper<String, String>(4);
      insertAll(wrapper, mem_map);
      checkAll(wrapper, mem_map);
      wrapper.close(); // clean up temporary files

      // default cache size (25000)
      wrapper = new HashMapWrapper<String, String>();
      insertAll(wrapper, mem_map);
      checkAll(wrapper, mem_map);
      wrapper.close(); // clean up temporary files

      // check mixed put/remove/get functions
      wrapper = new HashMapWrapper<String, String>(2);
      insertAll(wrapper, mem_map);
      wrapper.remove("k3"); // k3 is in HTree
      mem_map.remove("k3");
      assertTrue(mem_map.size() == 3);
      checkAll(wrapper, mem_map);

      wrapper.remove("k1");
      mem_map.remove("k1");
      checkAll(wrapper, mem_map);

      String v4 = wrapper.get("k4");
      assertTrue(v4 != null);
      assert (v4.equals("v4"));

      wrapper.remove("k4");
      mem_map.remove("k4");
      checkAll(wrapper, mem_map);

      wrapper.put("k5", "v5");
      mem_map.put("k5", "v5");
      checkAll(wrapper, mem_map);

      wrapper.put("k6", "v6");
      mem_map.put("k6", "v6");
      checkAll(wrapper, mem_map);

      wrapper.put("k6", "v61");
      mem_map.put("k6", "v61");
      checkAll(wrapper, mem_map);

      wrapper.remove("k6");
      mem_map.remove("k6");
      checkAll(wrapper, mem_map);

      // get k1, k2 to main memory
      wrapper.get("k1");
      wrapper.get("k2");
      // delete k1 so that cache is half empty
      wrapper.remove("k1");
      mem_map.remove("k1");
      // put new pair (k6, v7) so that it will be in persistent hash
      wrapper.put("k6", "v7");
      mem_map.put("k6", "v7");
      checkAll(wrapper, mem_map);

      // test clear
      wrapper.clear();
      mem_map.clear();
      checkAll(wrapper, mem_map);
      wrapper.close(); // clean up temporary files

      // insert 3,000 pairs random testing
      wrapper = new HashMapWrapper<String, String>(1000);
      for (int i = 0; i < 3000; ++i) {
        String k = "k" + i;
        String v = "v" + i;
        wrapper.put(k, v);
        mem_map.put(k, v);
      }
      checkAll(wrapper, mem_map);
      System.out.println("Finished inserting 3000 pairs.");

      // do 10,000 random get/remove operations
      Random rand = new Random(12345678);
      for (int i = 0; i < 10000; ++i) {
        int j = rand.nextInt(3000);
        String k = "k" + j;
        String v;

        int command = rand.nextInt(3);
        switch (command) {
        case 0: // remove
          // System.out.println("removing " + k);// uncomment this for debugging
          wrapper.remove(k);
          mem_map.remove(k);
          break;
        case 1: // get
          // System.out.println("getting " + k);// uncomment this for debugging
          v = wrapper.get(k);
          String v2 = mem_map.get(k);
          assertTrue(
              "one of them doesn't exists or different values from two hash tables",
              v == null && v2 == null || v.equals(v2));
          break;
        case 2: // put
          v = "v" + rand.nextInt(3000);
          // System.out.println("putting (" + k + ", " + v);// uncomment this
          // for debugging
          wrapper.put(k, v);
          mem_map.put(k, v);
          break;
        }
        // checkAll(wrapper, mem_map); // uncomment this for debugging
      }
      checkAll(wrapper, mem_map);
      wrapper.close(); // clean up temporary files
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.toString());
      assertTrue("Exception should not be thrown.", false);
    }
    System.out.println("TestHashMapWrapper successful");
  }

  private void insertAll(HashMapWrapper<String, String> hashTable,
      HashMap<String, String> map) throws HiveException {

    for (String k : map.keySet()) {
      String v = map.get(k);
      hashTable.put(k, v);
    }
  }

  private void checkAll(HashMapWrapper<String, String> hashTable,
      HashMap<String, String> map) throws HiveException {

    // check each item in the HashMapWrapper was actually inserted
    for (String k : hashTable.keySet()) {
      String map_val = hashTable.get(k);
      String val = map.get(k);
      assertTrue(
          "some HashMapWrapper value is not in main memory HashMap: map_val = "
          + map_val + "; val = " + val, map_val != null && val != null);
      assertTrue(
          "value in HashMapWrapper is not the same as MM HashMap: map_val = "
          + map_val + "; val = " + val, val.equals(map_val));
    }

    // check all inserted elements are in HashMapWrapper
    for (String k : map.keySet()) {
      String map_val = hashTable.get(k);
      String val = map.get(k);
      assertTrue("Some MM HashMap key is not in HashMapWrapper: map_val = "
          + map_val + "; val = " + val, map_val != null && val != null);
      assertTrue("Value in MM HashMap is not in HashMapWrapper: map_val = "
          + map_val + "; val = " + val, val.equals(map_val));
    }
  }
}
