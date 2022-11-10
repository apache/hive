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

import java.io.Closeable;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collection of Java class loading/reflection related utilities common across
 * Hive.
 */
public final class JavaUtils {
  private static final Logger LOG = LoggerFactory.getLogger(JavaUtils.class);

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

  public static Class loadClass(String shadePrefix, String className) throws ClassNotFoundException {
    return loadClass(shadePrefix + "." + className);
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
        String detailedMessage = current instanceof URLClassLoader ?
            Arrays.toString(((URLClassLoader) current).getURLs()) :
            "";
        LOG.info("Failed to close class loader " + current + " " + detailedMessage, e);
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

  public static void closeClassLoader(ClassLoader loader) throws IOException {
    if (loader instanceof Closeable) {
      ((Closeable) loader).close();
    } else {
      LOG.warn("Ignoring attempt to close class loader ({}) -- not instance of UDFClassLoader.",
          loader == null ? "mull" : loader.getClass().getSimpleName());
    }
  }

  /**
   * Utility method for ACID to normalize logging info.  Matches
   * org.apache.hadoop.hive.metastore.api.LockRequest#toString
   */
  public static String lockIdToString(long extLockId) {
    return "lockid:" + extLockId;
  }

  public static String txnIdToString(long txnId) {
    return "txnid:" + txnId;
  }

  public static String writeIdToString(long writeId) {
    return "writeid:" + writeId;
  }

  public static String txnIdsToString(List<Long> txnIds) {
    return "Transactions requested to be aborted: " + txnIds.toString();
  }

  private JavaUtils() {
    // prevent instantiation
  }

  public static Throwable findRootCause(Throwable throwable) {
    Throwable rootCause = throwable;
    while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
      rootCause = rootCause.getCause();
    }
    return rootCause;
  }
}
