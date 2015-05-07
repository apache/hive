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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import junit.framework.TestCase;

/**
 * Unit test for the vectorized conversion to and from row object[].
 */
public class TestDebugDisplay extends TestCase {

  public void testDebugDisplay() throws Throwable {

  try {
    String result;
    int[] test0 = {};
    result = VectorMapJoinGenerateResultOperator.intArrayToRangesString(test0, test0.length);
    System.out.println(result);
    int[] test1 = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31};
    result = VectorMapJoinGenerateResultOperator.intArrayToRangesString(test1, test1.length);
    System.out.println(result);
    int[] test2 = {5};
    result = VectorMapJoinGenerateResultOperator.intArrayToRangesString(test2, test2.length);
    System.out.println(result);
    int[] test3 = {4,4};
    result = VectorMapJoinGenerateResultOperator.intArrayToRangesString(test3, test3.length);
    System.out.println(result);
    int[] test4 = {0,1,2,3,4,5,6,6,7,7,8};
    result = VectorMapJoinGenerateResultOperator.intArrayToRangesString(test4, test4.length);
    System.out.println(result);
    int[] test5 = {0,0,1};
    result = VectorMapJoinGenerateResultOperator.intArrayToRangesString(test5, test5.length);
    System.out.println(result);
    int[] test6 = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,21,22,23,24,25,26,27,28,29,30,31};
    result = VectorMapJoinGenerateResultOperator.intArrayToRangesString(test6, test6.length);
    System.out.println(result);
    int[] test7 = {4,2};
    result = VectorMapJoinGenerateResultOperator.intArrayToRangesString(test7, test7.length);
    System.out.println(result);


  } catch (Throwable e) {
    e.printStackTrace();
    throw e;
  }
  }
}