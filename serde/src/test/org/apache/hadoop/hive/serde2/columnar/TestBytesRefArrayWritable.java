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

package org.apache.hadoop.hive.serde2.columnar;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestBytesRefArrayWritable {
  private final BytesRefArrayWritable left = new BytesRefArrayWritable(4);
  private final BytesRefArrayWritable right = new BytesRefArrayWritable(4);

  @Before
  public void setup() throws Exception {
    left.set(0, new BytesRefWritable("123".getBytes("UTF-8")));
    left.set(1, new BytesRefWritable("456".getBytes("UTF-8")));
    left.set(2, new BytesRefWritable("789".getBytes("UTF-8")));
    left.set(3, new BytesRefWritable("1000".getBytes("UTF-8")));

    right.set(0, new BytesRefWritable("123".getBytes("UTF-8")));
    right.set(1, new BytesRefWritable("456".getBytes("UTF-8")));
    right.set(2, new BytesRefWritable("289".getBytes("UTF-8")));
    right.set(3, new BytesRefWritable("1000".getBytes("UTF-8")));
  }

  @Test // HIVE-5839
  public void testCompareTo() {
    int a = left.compareTo(right);
    int b = right.compareTo(left);
    Assert.assertEquals("a.compareTo(b) should be equal to -b.compareTo(a)", a, -b );
    Assert.assertEquals("An object must be equal to itself", 0, left.compareTo(left));
  }

}
