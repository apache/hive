/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional debugrmation
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

package org.apache.hadoop.hive.llap;

import org.apache.hadoop.hive.ql.io.orc.EncodedReaderImpl;

/**
 * A class that contains debug methods; also allows enabling the logging of various
 * trace messages with low runtime cost, in order to investigate reproducible bugs.
 */
public class DebugUtils {

  public static boolean isTraceEnabled() {
    return false;
  }

  private final static boolean isTraceOrcEnabled = EncodedReaderImpl.LOG.isDebugEnabled();
  public static boolean isTraceOrcEnabled() {
    return isTraceOrcEnabled; // TODO: temporary, should be hardcoded false
  }

  public static boolean isTraceLockingEnabled() {
    return false;
  }

  public static boolean isTraceMttEnabled() {
    return false;
  }

  public static boolean isTraceCachingEnabled() {
    return false;
  }

  public static String toString(long[] a, int offset, int len) {
    StringBuilder b = new StringBuilder();
    b.append('[');
    for (int i = offset; i < offset + len; ++i) {
      b.append(a[i]);
      b.append(", ");
    }
    b.append(']');
    return b.toString();
  }

  public static String toString(byte[] a, int offset, int len) {
    StringBuilder b = new StringBuilder();
    b.append('[');
    for (int i = offset; i < offset + len; ++i) {
      b.append(a[i]);
      b.append(", ");
    }
    b.append(']');
    return b.toString();
  }

  public static String toString(boolean[] a) {
    StringBuilder b = new StringBuilder();
    b.append('[');
    for (int i = 0; i < a.length; ++i) {
      b.append(a[i] ? "1" : "0");
    }
    b.append(']');
    return b.toString();
  }
}
