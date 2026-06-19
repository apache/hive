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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomByteArrayStream {

  private Random random;
  private int min;

  private List<byte[]> byteArrays;

  public RandomByteArrayStream(Random random) {
    this.random = random;
    byteArrays = new ArrayList<byte[]>();
    min = 1;
  }

  public RandomByteArrayStream(Random random, int min) {
    this.random = random;
    byteArrays = new ArrayList<byte[]>();
    this.min = min;
  }

  public byte[] next() {
    int category = random.nextInt(100);
    int count = 0;
    if (category < 98) {
      count = min + random.nextInt(10);
    } else {
      switch (category - 98) {
      case 0:
        count = Math.max(min, 10) + random.nextInt(90);
        break;
      case 1:
        count = Math.max(min, 100) + random.nextInt(900);
      }
    }
    byte[] bytes = new byte[count];
    random.nextBytes(bytes);
    byteArrays.add(bytes);
    return bytes;
  }

  public int size() {
    return byteArrays.size();
  }

  public byte[] get(int i) {
    return byteArrays.get(i);
  }

  public boolean contains(byte[] bytes) {
    int length = bytes.length;
    for (int i = 0; i < byteArrays.size(); i++) {
      byte[] streamBytes = byteArrays.get(i);
      if (streamBytes.length != length) {
        continue;
      }
      boolean match = true;  // Assume
      for (int j = 0 ; j < length; j++) {
        if (streamBytes[j] != bytes[j]) {
          match = false;
          break;
        }
      }
      if (match) {
        return true;
      }
    }
    return false;
  }
}