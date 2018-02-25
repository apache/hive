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

package org.apache.hadoop.hive.common;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collection of Java class loading/reflection related utilities common across
 * Hive.
 */
public final class JavaUtils {

  public static final String BASE_PREFIX =  "base";
  public static final String DELTA_PREFIX = "delta";
  public static final String DELTA_DIGITS = "%07d";
  public static final int DELTA_DIGITS_LEN = 7;
  public static final String STATEMENT_DIGITS = "%04d";
  private static final Logger LOG = LoggerFactory.getLogger(JavaUtils.class);
  private static final Method SUN_MISC_UTIL_RELEASE;

  static {
    if (Closeable.class.isAssignableFrom(URLClassLoader.class)) {
      SUN_MISC_UTIL_RELEASE = null;
    } else {
      Method release = null;
      try {
        Class<?> clazz = Class.forName("sun.misc.ClassLoaderUtil");
        release = clazz.getMethod("releaseLoader", URLClassLoader.class);
      } catch (Exception e) {
        // ignore
      }
      SUN_MISC_UTIL_RELEASE = release;
    }
  }

  /**
   * Standard way of getting classloader in Hive code (outside of Hadoop).
   *
   * Uses the context loader to get access to classpaths to auxiliary and jars
   * added with 'add jar' command. Falls back to current classloader.
   *
   * In Hadoop-related code, we use Configuration.getClassLoader().
   */
  public static ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = JavaUtils.class.getClassLoader();
    }
    return classLoader;
  }

  public static Class loadClass(String className) throws ClassNotFoundException {
    return loadClass(className, true);
  }

  public static Class loadClass(String className, boolean init) throws ClassNotFoundException {
    return Class.forName(className, init, getClassLoader());
  }

  public static boolean closeClassLoadersTo(ClassLoader current, ClassLoader stop) {
    if (!isValidHierarchy(current, stop)) {
      return false;
    }
    for (; current != null && current != stop; current = current.getParent()) {
      try {
        closeClassLoader(current);
      } catch (IOException e) {
        LOG.info("Failed to close class loader " + current +
            Arrays.toString(((URLClassLoader) current).getURLs()), e);
      }
    }
    return true;
  }

  // check before closing loaders, not to close app-classloader, etc. by mistake
  private static boolean isValidHierarchy(ClassLoader current, ClassLoader stop) {
    if (current == null || stop == null || current == stop) {
      return false;
    }
    for (; current != null && current != stop; current = current.getParent()) {
    }
    return current == stop;
  }

  // best effort to close
  // see https://issues.apache.org/jira/browse/HIVE-3969 for detail
  public static void closeClassLoader(ClassLoader loader) throws IOException {
    if (loader instanceof Closeable) {
      ((Closeable)loader).close();
    } else if (SUN_MISC_UTIL_RELEASE != null && loader instanceof URLClassLoader) {
      PrintStream outputStream = System.out;
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      PrintStream newOutputStream = new PrintStream(byteArrayOutputStream);
      try {
        // SUN_MISC_UTIL_RELEASE.invoke prints to System.out
        // So we're changing the outputstream for that call,
        // and setting it back to original System.out when we're done
        System.setOut(newOutputStream);
        SUN_MISC_UTIL_RELEASE.invoke(null, loader);
        String output = byteArrayOutputStream.toString("UTF8");
        LOG.debug(output);
      } catch (InvocationTargetException e) {
        if (e.getTargetException() instanceof IOException) {
          throw (IOException)e.getTargetException();
        }
        throw new IOException(e.getTargetException());
      } catch (Exception e) {
        throw new IOException(e);
      }
      finally {
        System.setOut(outputStream);
        newOutputStream.close();
      }
    }
  }

  /**
   * Utility method for ACID to normalize logging info.  Matches
   * org.apache.hadoop.hive.metastore.api.LockRequest#toString
   */
  public static String lockIdToString(long extLockId) {
    return "lockid:" + extLockId;
  }
  /**
   * Utility method for ACID to normalize logging info.  Matches
   * org.apache.hadoop.hive.metastore.api.LockResponse#toString
   */
  public static String txnIdToString(long txnId) {
    return "txnid:" + txnId;
  }

  public static String txnIdsToString(List<Long> txnIds) {
    return "Transactions requested to be aborted: " + txnIds.toString();
  }

  private JavaUtils() {
    // prevent instantiation
  }

  public static Long extractTxnId(Path file) {
    String fileName = file.getName();
    String[] parts = fileName.split("_", 4);  // e.g. delta_0000001_0000001_0000 or base_0000022
    if (parts.length < 2 || !(DELTA_PREFIX.equals(parts[0]) || BASE_PREFIX.equals(parts[0]))) {
      LOG.debug("Cannot extract transaction ID for a MM table: " + file
          + " (" + Arrays.toString(parts) + ")");
      return null;
    }
    long writeId = -1;
    try {
      writeId = Long.parseLong(parts[1]);
    } catch (NumberFormatException ex) {
      LOG.debug("Cannot extract transaction ID for a MM table: " + file
          + "; parsing " + parts[1] + " got " + ex.getMessage());
      return null;
    }
    return writeId;
  }

  public static class IdPathFilter implements PathFilter {
    private String baseDirName, deltaDirName;
    private final boolean isMatch, isIgnoreTemp, isDeltaPrefix;

    public IdPathFilter(long writeId, int stmtId, boolean isMatch) {
      this(writeId, stmtId, isMatch, false);
    }

    public IdPathFilter(long writeId, int stmtId, boolean isMatch, boolean isIgnoreTemp) {
      String deltaDirName = null;
      deltaDirName = DELTA_PREFIX + "_" + String.format(DELTA_DIGITS, writeId) + "_" +
              String.format(DELTA_DIGITS, writeId) + "_";
      isDeltaPrefix = (stmtId < 0);
      if (!isDeltaPrefix) {
        deltaDirName += String.format(STATEMENT_DIGITS, stmtId);
      }

      this.baseDirName = BASE_PREFIX + "_" + String.format(DELTA_DIGITS, writeId);
      this.deltaDirName = deltaDirName;
      this.isMatch = isMatch;
      this.isIgnoreTemp = isIgnoreTemp;
    }

    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      if (name.equals(baseDirName) || (isDeltaPrefix && name.startsWith(deltaDirName))
          || (!isDeltaPrefix && name.equals(deltaDirName))) {
        return isMatch;
      }
      if (isIgnoreTemp && name.length() > 0) {
        char c = name.charAt(0);
        if (c == '.' || c == '_') return false; // Regardless of isMatch, ignore this.
      }
      return !isMatch;
    }
  }

  public static class AnyIdDirFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      if (!name.startsWith(DELTA_PREFIX + "_")) return false;
      String idStr = name.substring(DELTA_PREFIX.length() + 1, DELTA_PREFIX.length() + 1 + DELTA_DIGITS_LEN);
      try {
        Long.parseLong(idStr);
      } catch (NumberFormatException ex) {
        return false;
      }
      return true;
    }
  }
}
