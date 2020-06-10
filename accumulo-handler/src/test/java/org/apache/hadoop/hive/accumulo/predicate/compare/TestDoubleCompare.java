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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

public class TestDoubleCompare {

  private DoubleCompare doubleCompare;

  @Before
  public void setup() {
    doubleCompare = new DoubleCompare();
    byte[] db = new byte[8];
    ByteBuffer.wrap(db).putDouble(10.5d);
    doubleCompare.init(db);
  }

  public byte[] getBytes(double val) {
    byte[] dBytes = new byte[8];
    ByteBuffer.wrap(dBytes).putDouble(val);
    BigDecimal bd = doubleCompare.serialize(dBytes);
    assertEquals(bd.doubleValue(), val, 0);
    return dBytes;
  }

  @Test
  public void equal() {
    Equal equalObj = new Equal(doubleCompare);
    byte[] val = getBytes(10.5d);
    assertTrue(equalObj.accept(val));
  }

  @Test
  public void notEqual() {
    NotEqual notEqualObj = new NotEqual(doubleCompare);
    byte[] val = getBytes(11.0d);
    assertTrue(notEqualObj.accept(val));

    val = getBytes(10.5d);
    assertFalse(notEqualObj.accept(val));

  }

  @Test
  public void greaterThan() {
    GreaterThan greaterThanObj = new GreaterThan(doubleCompare);
    byte[] val = getBytes(11.0d);

    assertTrue(greaterThanObj.accept(val));

    val = getBytes(4.5d);
    assertFalse(greaterThanObj.accept(val));

    val = getBytes(10.5d);
    assertFalse(greaterThanObj.accept(val));
  }

  @Test
  public void greaterThanOrEqual() {
    GreaterThanOrEqual greaterThanOrEqualObj = new GreaterThanOrEqual(doubleCompare);

    byte[] val = getBytes(11.0d);

    assertTrue(greaterThanOrEqualObj.accept(val));

    val = getBytes(4.0d);
    assertFalse(greaterThanOrEqualObj.accept(val));

    val = getBytes(10.5d);
    assertTrue(greaterThanOrEqualObj.accept(val));
  }

  @Test
  public void lessThan() {

    LessThan lessThanObj = new LessThan(doubleCompare);

    byte[] val = getBytes(11.0d);

    assertFalse(lessThanObj.accept(val));

    val = getBytes(4.0d);
    assertTrue(lessThanObj.accept(val));

    val = getBytes(10.5d);
    assertFalse(lessThanObj.accept(val));

  }

  @Test
  public void lessThanOrEqual() {

    LessThanOrEqual lessThanOrEqualObj = new LessThanOrEqual(doubleCompare);

    byte[] val = getBytes(11.0d);

    assertFalse(lessThanOrEqualObj.accept(val));

    val = getBytes(4.0d);
    assertTrue(lessThanOrEqualObj.accept(val));

    val = getBytes(10.5d);
    assertTrue(lessThanOrEqualObj.accept(val));
  }

  @Test
  public void like() {
    try {
      Like likeObj = new Like(doubleCompare);
      assertTrue(likeObj.accept(new byte[] {}));
      fail("should not accept");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains(
          "Like not supported for " + doubleCompare.getClass().getName()));
    }
  }

  @Test
  public void invalidSerialization() {
    try {
      byte[] badVal = new byte[4];
      ByteBuffer.wrap(badVal).putInt(1);
      doubleCompare.serialize(badVal);
      fail("Should fail");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains(" occurred trying to build double value"));
    }
  }
}
