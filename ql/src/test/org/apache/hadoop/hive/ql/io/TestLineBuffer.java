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

package org.apache.hadoop.hive.ql.io;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * LineEndBuffer simple unit test.
 */
public class TestLineBuffer {

  @Test
  public void testLineEndBuffer() {
    SkippingTextInputFormat.LineBuffer buffer;
    buffer = new SkippingTextInputFormat.LineBuffer(3);
    buffer.consume(200, 200);
    buffer.consume(100, 200);
    buffer.consume(100, 100);
    buffer.consume(50, 100);
    assertEquals(0, buffer.getRemainingLineCount());
    assertEquals(50, buffer.getFirstLineStart());

    buffer = new SkippingTextInputFormat.LineBuffer(3);
    buffer.consume(200, 200);
    buffer.consume(150, 200);
    buffer.consume(100, 200);
    assertEquals(0, buffer.getRemainingLineCount());
    assertEquals(100, buffer.getFirstLineStart());

    buffer = new SkippingTextInputFormat.LineBuffer(3);
    buffer.consume(200, 200);
    assertEquals(2, buffer.getRemainingLineCount());
    buffer.consume(100, 100);
    assertEquals(1, buffer.getRemainingLineCount());
    buffer.consume(50, 100);
    assertEquals(0, buffer.getRemainingLineCount());
    assertEquals(50, buffer.getFirstLineStart());

    buffer = new SkippingTextInputFormat.LineBuffer(3);
    buffer.consume(200, 200);
    assertEquals(2, buffer.getRemainingLineCount());
    buffer.consume(50, 100);
    assertEquals(1, buffer.getRemainingLineCount());
    buffer.consume(25, 100);
    assertEquals(0, buffer.getRemainingLineCount());
    assertEquals(25, buffer.getFirstLineStart());
  }
}
