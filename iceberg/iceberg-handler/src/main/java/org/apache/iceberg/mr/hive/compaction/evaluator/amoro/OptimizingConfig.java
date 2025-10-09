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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

/** Configuration for optimizing process scheduling and executing. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OptimizingConfig {

  // self-optimizing.enabled
  private boolean enabled;

  // self-optimizing.target-size
  private long targetSize;

  // read.split.open-file-cost
  private long openFileCost;

  // self-optimizing.fragment-ratio
  private int fragmentRatio;

  // self-optimizing.min-target-size-ratio
  private double minTargetSizeRatio;

  // self-optimizing.minor.trigger.file-count
  private int minorLeastFileCount;

  // self-optimizing.minor.trigger.interval
  private int minorLeastInterval;

  // self-optimizing.major.trigger.duplicate-ratio
  private double majorDuplicateRatio;

  // self-optimizing.full.trigger.interval
  private int fullTriggerInterval;

  // self-optimizing.full.rewrite-all-files
  private boolean fullRewriteAllFiles;

  public OptimizingConfig() {
  }

  public boolean isEnabled() {
    return enabled;
  }

  public OptimizingConfig setEnabled(boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  public long getTargetSize() {
    return targetSize;
  }

  public OptimizingConfig setTargetSize(long targetSize) {
    this.targetSize = targetSize;
    return this;
  }

  public long getOpenFileCost() {
    return openFileCost;
  }

  public OptimizingConfig setOpenFileCost(long openFileCost) {
    this.openFileCost = openFileCost;
    return this;
  }

  public int getFragmentRatio() {
    return fragmentRatio;
  }

  public OptimizingConfig setFragmentRatio(int fragmentRatio) {
    this.fragmentRatio = fragmentRatio;
    return this;
  }

  public double getMinTargetSizeRatio() {
    return minTargetSizeRatio;
  }

  public OptimizingConfig setMinTargetSizeRatio(double minTargetSizeRatio) {
    this.minTargetSizeRatio = minTargetSizeRatio;
    return this;
  }

  public int getMinorLeastFileCount() {
    return minorLeastFileCount;
  }

  public OptimizingConfig setMinorLeastFileCount(int minorLeastFileCount) {
    this.minorLeastFileCount = minorLeastFileCount;
    return this;
  }

  public int getMinorLeastInterval() {
    return minorLeastInterval;
  }

  public OptimizingConfig setMinorLeastInterval(int minorLeastInterval) {
    this.minorLeastInterval = minorLeastInterval;
    return this;
  }

  public double getMajorDuplicateRatio() {
    return majorDuplicateRatio;
  }

  public OptimizingConfig setMajorDuplicateRatio(double majorDuplicateRatio) {
    this.majorDuplicateRatio = majorDuplicateRatio;
    return this;
  }

  public int getFullTriggerInterval() {
    return fullTriggerInterval;
  }

  public OptimizingConfig setFullTriggerInterval(int fullTriggerInterval) {
    this.fullTriggerInterval = fullTriggerInterval;
    return this;
  }

  public boolean isFullRewriteAllFiles() {
    return fullRewriteAllFiles;
  }

  public OptimizingConfig setFullRewriteAllFiles(boolean fullRewriteAllFiles) {
    this.fullRewriteAllFiles = fullRewriteAllFiles;
    return this;
  }

  public static OptimizingConfig parse(Map<String, String> properties) {
    return new OptimizingConfig()
        .setEnabled(
            CompatiblePropertyUtil.propertyAsBoolean(
                properties,
                TableProperties.ENABLE_SELF_OPTIMIZING,
                TableProperties.ENABLE_SELF_OPTIMIZING_DEFAULT))
        .setFragmentRatio(
            CompatiblePropertyUtil.propertyAsInt(
                properties,
                TableProperties.SELF_OPTIMIZING_FRAGMENT_RATIO,
                TableProperties.SELF_OPTIMIZING_FRAGMENT_RATIO_DEFAULT))
        .setMinTargetSizeRatio(
            CompatiblePropertyUtil.propertyAsDouble(
                properties,
                TableProperties.SELF_OPTIMIZING_MIN_TARGET_SIZE_RATIO,
                TableProperties.SELF_OPTIMIZING_MIN_TARGET_SIZE_RATIO_DEFAULT))
        .setOpenFileCost(
            CompatiblePropertyUtil.propertyAsLong(
                properties,
                TableProperties.SPLIT_OPEN_FILE_COST,
                TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT))
        .setTargetSize(
            CompatiblePropertyUtil.propertyAsLong(
                properties,
                TableProperties.SELF_OPTIMIZING_TARGET_SIZE,
                TableProperties.SELF_OPTIMIZING_TARGET_SIZE_DEFAULT))
        .setMinorLeastFileCount(
            CompatiblePropertyUtil.propertyAsInt(
                properties,
                TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_FILE_CNT,
                TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_FILE_CNT_DEFAULT))
        .setMinorLeastInterval(
            CompatiblePropertyUtil.propertyAsInt(
                properties,
                TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_INTERVAL,
                TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_INTERVAL_DEFAULT))
        .setMajorDuplicateRatio(
            CompatiblePropertyUtil.propertyAsDouble(
                properties,
                TableProperties.SELF_OPTIMIZING_MAJOR_TRIGGER_DUPLICATE_RATIO,
                TableProperties.SELF_OPTIMIZING_MAJOR_TRIGGER_DUPLICATE_RATIO_DEFAULT))
        .setFullTriggerInterval(
            CompatiblePropertyUtil.propertyAsInt(
                properties,
                TableProperties.SELF_OPTIMIZING_FULL_TRIGGER_INTERVAL,
                TableProperties.SELF_OPTIMIZING_FULL_TRIGGER_INTERVAL_DEFAULT))
        .setFullRewriteAllFiles(
            CompatiblePropertyUtil.propertyAsBoolean(
                properties,
                TableProperties.SELF_OPTIMIZING_FULL_REWRITE_ALL_FILES,
                TableProperties.SELF_OPTIMIZING_FULL_REWRITE_ALL_FILES_DEFAULT));
  }
}
