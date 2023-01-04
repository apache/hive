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

import java.util.regex.Pattern;

/**
 * Common constants for ACID tables to use in HMS and HS2.
 */
public class AcidConstants {

  public static final String BASE_PREFIX = "base_";
  public static final String DELTA_PREFIX = "delta_";
  public static final String DELETE_DELTA_PREFIX = "delete_delta_";

  public static final String BUCKET_DIGITS = "%05d";
  public static final String LEGACY_FILE_BUCKET_DIGITS = "%06d";
  public static final String DELTA_DIGITS = "%07d";
  /**
   * 10K statements per tx.  Probably overkill ... since that many delta files
   * would not be good for performance
   */
  public static final String STATEMENT_DIGITS = "%04d";

  public static final String VISIBILITY_PREFIX = "_v";
  public static final Pattern VISIBILITY_PATTERN = Pattern.compile(VISIBILITY_PREFIX + "\\d+");

  public static final String SOFT_DELETE_PATH_SUFFIX = ".v";
  public static final String SOFT_DELETE_TABLE_PATTERN = "\\" + SOFT_DELETE_PATH_SUFFIX + "\\d+";
  public static final String SOFT_DELETE_TABLE = "soft_delete";
  
  public static String baseDir(long writeId) {
    return BASE_PREFIX + String.format(DELTA_DIGITS, writeId);
  }

}
