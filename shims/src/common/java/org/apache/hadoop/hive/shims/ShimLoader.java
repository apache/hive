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
package org.apache.hadoop.hive.shims;

import org.apache.hadoop.util.VersionInfo;
import java.util.Map;
import java.util.HashMap;

public abstract class ShimLoader {
  private static HadoopShims hadoopShims;
  private static JettyShims jettyShims;

  /**
   * The names of the classes for shimming Hadoop for each major version.
   */
  private static HashMap<String, String> HADOOP_SHIM_CLASSES =
    new HashMap<String, String>();

  static {
    HADOOP_SHIM_CLASSES.put("0.17", "org.apache.hadoop.hive.shims.Hadoop17Shims");
    HADOOP_SHIM_CLASSES.put("0.18", "org.apache.hadoop.hive.shims.Hadoop18Shims");
    HADOOP_SHIM_CLASSES.put("0.19", "org.apache.hadoop.hive.shims.Hadoop19Shims");
    HADOOP_SHIM_CLASSES.put("0.20", "org.apache.hadoop.hive.shims.Hadoop20Shims");
  }

  /**
   * The names of the classes for shimming Jetty for each major version of
   * Hadoop.
   */
  private static HashMap<String, String> JETTY_SHIM_CLASSES =
    new HashMap<String, String>();

  static {
    JETTY_SHIM_CLASSES.put("0.17", "org.apache.hadoop.hive.shims.Jetty17Shims");
    JETTY_SHIM_CLASSES.put("0.18", "org.apache.hadoop.hive.shims.Jetty18Shims");
    JETTY_SHIM_CLASSES.put("0.19", "org.apache.hadoop.hive.shims.Jetty19Shims");
    JETTY_SHIM_CLASSES.put("0.20", "org.apache.hadoop.hive.shims.Jetty20Shims");
  }


  /**
   * Factory method to get an instance of HadoopShims based on the
   * version of Hadoop on the classpath.
   */
  public synchronized static HadoopShims getHadoopShims() {
    if (hadoopShims == null) {
      hadoopShims = loadShims(HADOOP_SHIM_CLASSES, HadoopShims.class);
    }
    return hadoopShims;
  }

  /**
   * Factory method to get an instance of JettyShims based on the version
   * of Hadoop on the classpath.
   */
  public synchronized static JettyShims getJettyShims() {
    if (jettyShims == null) {
      jettyShims = loadShims(JETTY_SHIM_CLASSES, JettyShims.class);
    }
    return jettyShims;
  }

  @SuppressWarnings("unchecked")
  private static <T> T loadShims(Map<String, String> classMap, Class<T> xface) {
    String vers = getMajorVersion();
    String className = classMap.get(vers);
    try {
      Class clazz = Class.forName(className);
      return xface.cast(clazz.newInstance());
    } catch (Exception e) {
      throw new RuntimeException("Could not load shims in class " +
                                 className, e);
    }
  }

  /**
   * Return the major version of Hadoop currently on the classpath.
   * This is simply the first two components of the version number
   * (e.g "0.18" or "0.20")
   */
  private static String getMajorVersion() {
    String vers = VersionInfo.getVersion();

    String parts[] = vers.split("\\.");
    if (parts.length < 2) {
      throw new RuntimeException("Illegal Hadoop Version: " + vers +
                                 " (expected A.B.* format)");
    }
    return parts[0] + "." + parts[1];
  }
}
