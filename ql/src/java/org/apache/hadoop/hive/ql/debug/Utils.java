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

package org.apache.hadoop.hive.ql.debug;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import org.apache.commons.lang.StringUtils;

import com.sun.management.HotSpotDiagnosticMXBean;

/**
 * Debug utility methods for Hive.
 */
public class Utils {
  private static final String HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";
  private static volatile HotSpotDiagnosticMXBean hotspotMBean;

  /**
   * Dumps process heap to a file in temp directoty.
   * @param args Strings to use to build a file name (dump_arg0_arg1_....).
   */
  public static void dumpHeapToTmp(String... args) {
    String tmpDir = System.getProperty("java.io.tmpdir");
    if (StringUtils.isBlank(tmpDir)) {
      tmpDir = "/tmp/";
    }
    String fileName =  tmpDir + File.pathSeparatorChar + "dump";
    for (String arg : args) {
      fileName += "_" + arg;
    }
    fileName += "_" + System.nanoTime() + ".hprof";
    dumpHeap(fileName, true);
  }

  /**
   * Dumps process heap.
   * @param fileName File name to use.
   * @param live Whether to only dump live objects.
   */
  public static void dumpHeap(String fileName, boolean live) {
    if (hotspotMBean == null) {
      try {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        hotspotMBean = ManagementFactory.newPlatformMXBeanProxy(server,
            HOTSPOT_BEAN_NAME, HotSpotDiagnosticMXBean.class);
      } catch (RuntimeException re) {
          throw re;
      } catch (Exception exp) {
          throw new RuntimeException(exp);
      }
    }
    try {
        hotspotMBean.dumpHeap(fileName, live);
    } catch (RuntimeException re) {
        throw re;
    } catch (Exception exp) {
        throw new RuntimeException(exp);
    }
  }

  /** 
   * Outputs some bytes as hex w/printable characters prints.
   * Helpful debug method; c/p from HBase Bytes.
   * @param b Bytes.
   * @param off Offset.
   * @param len Length.
   * @return The string representation.
   */
  public static String toStringBinary(final byte [] b, int off, int len) {
    StringBuilder result = new StringBuilder();
    try {
      String first = new String(b, off, len, "ISO-8859-1");
      for (int i = 0; i < first.length() ; ++i ) {
        int ch = first.charAt(i) & 0xFF;
        if ( (ch >= '0' && ch <= '9')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= 'a' && ch <= 'z')
            || " `~!@#$%^&*()-_=+[]{}\\|;:'\",.<>/?".indexOf(ch) >= 0 ) {
          result.append(first.charAt(i));
        } else {
          result.append(String.format("\\x%02X", ch));
        }
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("ISO-8859-1 not supported?", e);
    }
    return result.toString();
  }
}
