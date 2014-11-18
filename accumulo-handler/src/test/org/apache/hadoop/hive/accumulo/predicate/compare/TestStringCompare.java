/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.accumulo.predicate.compare;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class TestStringCompare {

  private StringCompare strCompare;

  @Before
  public void setup() {
    strCompare = new StringCompare();
    strCompare.init("aaa".getBytes());
  }

  @Test
  public void equal() {
    Equal equalObj = new Equal(strCompare);
    byte[] val = "aaa".getBytes();
    assertTrue(equalObj.accept(val));
  }

  @Test
  public void notEqual() {
    NotEqual notEqualObj = new NotEqual(strCompare);
    byte[] val = "aab".getBytes();
    assertTrue(notEqualObj.accept(val));

    val = "aaa".getBytes();
    assertFalse(notEqualObj.accept(val));

  }

  @Test
  public void greaterThan() {
    GreaterThan greaterThanObj = new GreaterThan(strCompare);
    byte[] val = "aab".getBytes();

    assertTrue(greaterThanObj.accept(val));

    val = "aa".getBytes();
    assertFalse(greaterThanObj.accept(val));

    val = "aaa".getBytes();
    assertFalse(greaterThanObj.accept(val));
  }

  @Test
  public void greaterThanOrEqual() {
    GreaterThanOrEqual greaterThanOrEqualObj = new GreaterThanOrEqual(strCompare);
    byte[] val = "aab".getBytes();

    assertTrue(greaterThanOrEqualObj.accept(val));

    val = "aa".getBytes();
    assertFalse(greaterThanOrEqualObj.accept(val));

    val = "aaa".getBytes();
    assertTrue(greaterThanOrEqualObj.accept(val));
  }

  @Test
  public void lessThan() {

    LessThan lessThanObj = new LessThan(strCompare);

    byte[] val = "aab".getBytes();

    assertFalse(lessThanObj.accept(val));

    val = "aa".getBytes();
    assertTrue(lessThanObj.accept(val));

    val = "aaa".getBytes();
    assertFalse(lessThanObj.accept(val));

  }

  @Test
  public void lessThanOrEqual() {

    LessThanOrEqual lessThanOrEqualObj = new LessThanOrEqual(strCompare);

    byte[] val = "aab".getBytes();

    assertFalse(lessThanOrEqualObj.accept(val));

    val = "aa".getBytes();
    assertTrue(lessThanOrEqualObj.accept(val));

    val = "aaa".getBytes();
    assertTrue(lessThanOrEqualObj.accept(val));
  }

  @Test
  public void like() {
    Like likeObj = new Like(strCompare);
    String condition = "%a";
    assertTrue(likeObj.accept(condition.getBytes()));

    condition = "%a%";
    assertTrue(likeObj.accept(condition.getBytes()));

    condition = "a%";
    assertTrue(likeObj.accept(condition.getBytes()));

    condition = "a%aa";
    assertFalse(likeObj.accept(condition.getBytes()));

    condition = "b%";
    assertFalse(likeObj.accept(condition.getBytes()));

    condition = "%ab%";
    assertFalse(likeObj.accept(condition.getBytes()));

    condition = "%ba";
    assertFalse(likeObj.accept(condition.getBytes()));
  }
}
