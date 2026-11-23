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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.serde2.geospatial;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

public class TestWkbValidator {

  /** Minimal WKB writer for building test inputs, including invalid ones. */
  private static final class Wkb {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteOrder order;

    Wkb(ByteOrder order) {
      this.order = order;
    }

    static Wkb le() {
      return new Wkb(ByteOrder.LITTLE_ENDIAN);
    }

    Wkb header(long type) {
      out.write(order == ByteOrder.LITTLE_ENDIAN ? 1 : 0);
      return count(type);
    }

    Wkb count(long value) {
      out.writeBytes(ByteBuffer.allocate(4).order(order).putInt((int) value).array());
      return this;
    }

    Wkb coords(double... values) {
      for (double value : values) {
        out.writeBytes(ByteBuffer.allocate(8).order(order).putDouble(value).array());
      }
      return this;
    }

    Wkb raw(byte[] bytes) {
      out.writeBytes(bytes);
      return this;
    }

    byte[] bytes() {
      return out.toByteArray();
    }
  }

  private static final double[] SQUARE = {0, 0, 1, 0, 1, 1, 0, 1, 0, 0};

  private static byte[] point(double x, double y) {
    return Wkb.le().header(1).coords(x, y).bytes();
  }

  private static void assertInvalid(byte[] wkb, String reason) {
    IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
        () -> WkbValidator.validate(wkb));
    Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("Invalid WKB: "));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains(reason));
  }

  @Test
  public void testValidGeometries() {
    WkbValidator.validate(point(1, 2));
    WkbValidator.validate(new Wkb(ByteOrder.BIG_ENDIAN).header(1).coords(1, 2).bytes());
    WkbValidator.validate(point(Double.NaN, Double.NaN));
    WkbValidator.validate(Wkb.le().header(1001).coords(1, 2, 3).bytes());
    WkbValidator.validate(Wkb.le().header(2001).coords(1, 2, 3).bytes());
    WkbValidator.validate(Wkb.le().header(3001).coords(1, 2, 3, 4).bytes());
    WkbValidator.validate(Wkb.le().header(2).count(0).bytes());
    WkbValidator.validate(Wkb.le().header(2).count(2).coords(0, 0, 1, 1).bytes());
    WkbValidator.validate(Wkb.le().header(3).count(0).bytes());
    WkbValidator.validate(Wkb.le().header(3).count(1).count(5).coords(SQUARE).bytes());
    WkbValidator.validate(Wkb.le().header(4).count(2).raw(point(1, 2))
        .raw(new Wkb(ByteOrder.BIG_ENDIAN).header(1).coords(3, 4).bytes()).bytes());
    WkbValidator.validate(Wkb.le().header(6).count(1)
        .header(3).count(1).count(5).coords(SQUARE).bytes());
    WkbValidator.validate(Wkb.le().header(7).count(2).raw(point(1, 2))
        .header(7).count(1).header(2).count(2).coords(0, 0, 1, 1).bytes());
  }

  @Test
  public void testOffsetAndLength() {
    byte[] wkb = point(1, 2);
    byte[] padded = new byte[wkb.length + 6];
    System.arraycopy(wkb, 0, padded, 3, wkb.length);
    WkbValidator.validate(padded, 3, wkb.length);

    IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
        () -> WkbValidator.validate(padded, 3, wkb.length - 1));
    Assert.assertEquals("Invalid WKB: unexpected end of input at position 6", e.getMessage());
  }

  @Test
  public void testMalformedInput() {
    assertInvalid(new byte[0], "empty input");
    assertInvalid(new byte[] {2, 1, 0, 0, 0}, "invalid byte order 2 at position 1");
    assertInvalid(new byte[] {1, 1, 0}, "unexpected end of input at position 2");
    assertInvalid(Wkb.le().header(1).coords(1).bytes(), "unexpected end of input");
    assertInvalid(Wkb.le().raw(point(1, 2)).raw(new byte[] {0}).bytes(), "unexpected trailing bytes at position 22");
    assertInvalid(Wkb.le().header(2).count(Integer.MAX_VALUE).bytes(), "exceeds the remaining bytes");
    assertInvalid("POINT (1 2)".getBytes(StandardCharsets.UTF_8), "invalid byte order");
  }

  @Test
  public void testUnsupportedTypes() {
    assertInvalid(Wkb.le().header(0).bytes(), "invalid or unsupported geometry type 0");
    assertInvalid(Wkb.le().header(8).bytes(), "invalid or unsupported geometry type 8");
    assertInvalid(Wkb.le().header(4001).coords(1, 2).bytes(), "invalid or unsupported geometry type 4001");
    assertInvalid(Wkb.le().header(0x20000001L).count(4326).coords(1, 2).bytes(), "extended WKB (EWKB)");
    assertInvalid(Wkb.le().header(0x80000001L).coords(1, 2, 3).bytes(), "extended WKB (EWKB)");
  }

  @Test
  public void testInvalidStructure() {
    assertInvalid(Wkb.le().header(2).count(1).coords(0, 0).bytes(), "zero or at least two points");
    assertInvalid(Wkb.le().header(3).count(1).count(3).coords(0, 0, 1, 0, 0, 0).bytes(), "at least four points");
    assertInvalid(Wkb.le().header(3).count(1).count(4).coords(0, 0, 1, 0, 1, 1, 0, 1).bytes(), "must be closed");
    assertInvalid(Wkb.le().header(4).count(1).header(2).count(0).bytes(), "geometry type 2 is not allowed here");
    assertInvalid(Wkb.le().header(7).count(1).header(1001).coords(1, 2, 3).bytes(), "same dimension");
  }

  @Test
  public void testCoordinates() {
    assertInvalid(point(Double.POSITIVE_INFINITY, 0), "coordinates must be finite");
    assertInvalid(Wkb.le().header(2).count(2).coords(0, 0, Double.NaN, 1).bytes(), "coordinates must be finite");
  }

  @Test
  public void testNestingDepthIsBounded() {
    Wkb wkb = Wkb.le();
    for (int i = 0; i < 100; i++) {
      wkb.header(7).count(1);
    }
    wkb.raw(point(1, 2));
    assertInvalid(wkb.bytes(), "nested deeper than");
  }
}
