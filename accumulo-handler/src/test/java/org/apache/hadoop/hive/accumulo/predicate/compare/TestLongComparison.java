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

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

public class TestLongComparison {

  private LongCompare longComp;

  @Before
  public void setup() {
    byte[] lBytes = new byte[8];
    ByteBuffer.wrap(lBytes).putLong(10l);
    longComp = new LongCompare();
    longComp.init(lBytes);
  }

  public byte[] getBytes(long val) {
    byte[] lonBytes = new byte[8];
    ByteBuffer.wrap(lonBytes).putLong(val);
    long lon = longComp.serialize(lonBytes);
    assertEquals(lon, val);
    return lonBytes;
  }

  @Test
  public void equal() {
    Equal equalObj = new Equal(longComp);
    byte[] val = getBytes(10l);
    assertTrue(equalObj.accept(val));
  }

  @Test
  public void notEqual() {
    NotEqual notEqualObj = new NotEqual(longComp);
    byte[] val = getBytes(11l);
    assertTrue(notEqualObj.accept(val));

    val = getBytes(10l);
    assertFalse(notEqualObj.accept(val));

  }

  @Test
  public void greaterThan() {
    GreaterThan greaterThanObj = new GreaterThan(longComp);
    byte[] val = getBytes(11l);

    assertTrue(greaterThanObj.accept(val));

    val = getBytes(4l);
    assertFalse(greaterThanObj.accept(val));

    val = getBytes(10l);
    assertFalse(greaterThanObj.accept(val));
  }

  @Test
  public void greaterThanOrEqual() {
    GreaterThanOrEqual greaterThanOrEqualObj = new GreaterThanOrEqual(longComp);

    byte[] val = getBytes(11l);

    assertTrue(greaterThanOrEqualObj.accept(val));

    val = getBytes(4l);
    assertFalse(greaterThanOrEqualObj.accept(val));

    val = getBytes(10l);
    assertTrue(greaterThanOrEqualObj.accept(val));
  }

  @Test
  public void lessThan() {

    LessThan lessThanObj = new LessThan(longComp);

    byte[] val = getBytes(11l);

    assertFalse(lessThanObj.accept(val));

    val = getBytes(4l);
    assertTrue(lessThanObj.accept(val));

    val = getBytes(10l);
    assertFalse(lessThanObj.accept(val));

  }

  @Test
  public void lessThanOrEqual() {

    LessThanOrEqual lessThanOrEqualObj = new LessThanOrEqual(longComp);

    byte[] val = getBytes(11l);

    assertFalse(lessThanOrEqualObj.accept(val));

    val = getBytes(4l);
    assertTrue(lessThanOrEqualObj.accept(val));

    val = getBytes(10l);
    assertTrue(lessThanOrEqualObj.accept(val));
  }

  @Test
  public void like() {
    try {
      Like likeObj = new Like(longComp);
      assertTrue(likeObj.accept(new byte[] {}));
      fail("should not accept");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("Like not supported for " + longComp.getClass().getName()));
    }
  }

  @Test
  public void invalidSerialization() {
    try {
      byte[] badVal = new byte[4];
      ByteBuffer.wrap(badVal).putInt(1);
      longComp.serialize(badVal);
      fail("Should fail");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains(" occurred trying to build long value"));
    }
  }

}
