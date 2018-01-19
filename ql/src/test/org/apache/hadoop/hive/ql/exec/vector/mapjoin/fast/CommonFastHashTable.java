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

import java.util.Random;

public class CommonFastHashTable {

  protected static final float LOAD_FACTOR = 0.75f;
  protected static final int CAPACITY = 8;
  protected static final int WB_SIZE = 128; // Make sure we cross some buffer boundaries...
  protected static final int MODERATE_WB_SIZE = 8 * 1024;
  protected static final int MODERATE_CAPACITY = 512;
  protected static final int LARGE_WB_SIZE = 1024 * 1024;
  protected static final int LARGE_CAPACITY = 8388608;
  protected static Random random;

  protected static final int MAX_KEY_LENGTH = 100;

  protected static final int MAX_VALUE_LENGTH = 1000;

  public static int generateLargeCount() {
    int count = 0;
    if (random.nextInt(100) != 0) {
      switch (random.nextInt(5)) {
      case 0:
        count = 1;
        break;
      case 1:
        count = 2;
        break;
      case 2:
        count = 3;
      case 3:
        count = 4 + random.nextInt(7);
        break;
      case 4:
        count = 10 + random.nextInt(90);
        break;
      default:
        throw new Error("Missing case");
      }
    } else {
      switch (random.nextInt(3)) {
      case 0:
        count = 100 + random.nextInt(900);
        break;
      case 1:
        count = 1000 + random.nextInt(9000);
        break;
      case 2:
        count = 10000 + random.nextInt(90000);
        break;
      }
    }
    return count;
  }
}