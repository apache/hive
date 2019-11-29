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

package org.apache.hadoop.hive.ql;

import java.util.Set;

public class QTestSystemProperties {
  public static final String SYS_PROP_TMP_DIR_PROPERTY = "test.tmp.dir"; // typically target/tmp
  private static final String SYS_PROP_SRC_TABLES_PROPERTY = "test.src.tables";
  private static final String SYS_PROP_OUTPUT_OVERWRITE = "test.output.overwrite";
  private static final String SYS_PROP_SRC_UDFS = "test.src.udfs";
  private static final String SYS_PROP_VECTORIZATION_ENABLED = "test.vectorization.enabled";
  private static final String SYS_PROP_CHECK_SYNTAX = "test.check.syntax";
  private static final String SYS_PROP_FORCE_EXCLUSIONS = "test.force.exclusions";
  private static final String SYS_PROP_METASTORE_DB = "test.metastore.db";
  private static final String SYS_PROP_BUILD_DIR = "build.dir"; // typically target

  public static String getTempDir() {
    return System.getProperty(SYS_PROP_TMP_DIR_PROPERTY);
  }

  public static String[] getSrcTables() {
    return System.getProperty(SYS_PROP_SRC_TABLES_PROPERTY, "").trim().split(",");
  }

  public static void setSrcTables(Set<String> srcTables) {
    System.setProperty(SYS_PROP_SRC_TABLES_PROPERTY, String.join(",", srcTables));
  }

  public static String[] getSourceUdfs(String defaultTestSrcUDFs) {
    return System.getProperty(SYS_PROP_SRC_UDFS, defaultTestSrcUDFs).trim().split(",");
  }

  public static String getBuildDir() {
    return System.getProperty(SYS_PROP_BUILD_DIR);
  }

  public static String getMetaStoreDb() {
    return System.getProperty(SYS_PROP_METASTORE_DB) == null ? null
      : System.getProperty(SYS_PROP_METASTORE_DB).toLowerCase();
  }

  public static boolean isVectorizationEnabled() {
    return isTrue(SYS_PROP_VECTORIZATION_ENABLED);
  }

  public static boolean shouldOverwriteResults() {
    return isTrue(SYS_PROP_OUTPUT_OVERWRITE);
  }

  public static boolean shouldCheckSyntax() {
    return isTrue(SYS_PROP_CHECK_SYNTAX);
  }

  public static boolean shouldForceExclusions() {
    return isTrue(SYS_PROP_FORCE_EXCLUSIONS);
  }

  private static boolean isTrue(String propertyName) {
    return "true".equals(System.getProperty(propertyName));
  }
}