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

package org.apache.iceberg.mr.hive.compaction.evaluator.amoro;

/** Reserved mixed-format table properties list. */
public class TableProperties {

  private TableProperties() {
  }

  /** table optimize related properties */
  public static final String ENABLE_SELF_OPTIMIZING = "self-optimizing.enabled";
  public static final boolean ENABLE_SELF_OPTIMIZING_DEFAULT = true;
  public static final String SELF_OPTIMIZING_TARGET_SIZE = "self-optimizing.target-size";
  public static final long SELF_OPTIMIZING_TARGET_SIZE_DEFAULT = 134217728; // 128 MB
  public static final String SELF_OPTIMIZING_FRAGMENT_RATIO = "self-optimizing.fragment-ratio";
  public static final int SELF_OPTIMIZING_FRAGMENT_RATIO_DEFAULT = 8;
  public static final String SELF_OPTIMIZING_MIN_TARGET_SIZE_RATIO = "self-optimizing.min-target-size-ratio";
  public static final double SELF_OPTIMIZING_MIN_TARGET_SIZE_RATIO_DEFAULT = 0.75;
  public static final String SELF_OPTIMIZING_MINOR_TRIGGER_FILE_CNT = "self-optimizing.minor.trigger.file-count";
  public static final int SELF_OPTIMIZING_MINOR_TRIGGER_FILE_CNT_DEFAULT = 12;
  public static final String SELF_OPTIMIZING_MINOR_TRIGGER_INTERVAL = "self-optimizing.minor.trigger.interval";
  public static final int SELF_OPTIMIZING_MINOR_TRIGGER_INTERVAL_DEFAULT = 3600000; // 1 h
  public static final String SELF_OPTIMIZING_MAJOR_TRIGGER_DUPLICATE_RATIO =
      "self-optimizing.major.trigger.duplicate-ratio";
  public static final double SELF_OPTIMIZING_MAJOR_TRIGGER_DUPLICATE_RATIO_DEFAULT = 0.1;
  public static final String SELF_OPTIMIZING_FULL_TRIGGER_INTERVAL = "self-optimizing.full.trigger.interval";
  public static final int SELF_OPTIMIZING_FULL_TRIGGER_INTERVAL_DEFAULT = -1; // not trigger
  public static final String SELF_OPTIMIZING_FULL_REWRITE_ALL_FILES = "self-optimizing.full.rewrite-all-files";
  public static final boolean SELF_OPTIMIZING_FULL_REWRITE_ALL_FILES_DEFAULT = true;

  @Deprecated public static final String ENABLE_OPTIMIZE = "optimize.enable";
  @Deprecated public static final String FULL_OPTIMIZE_TRIGGER_MAX_INTERVAL = "optimize.full.trigger.max-interval";
  @Deprecated public static final String MINOR_OPTIMIZE_TRIGGER_MAX_INTERVAL = "optimize.minor.trigger.max-interval";
  @Deprecated public static final String MINOR_OPTIMIZE_TRIGGER_DELETE_FILE_COUNT =
      "optimize.minor.trigger.delete-file-count";
  public static final String SPLIT_OPEN_FILE_COST = org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
  public static final long SPLIT_OPEN_FILE_COST_DEFAULT = 4 * 1024 * 1024; // 4MB
}
