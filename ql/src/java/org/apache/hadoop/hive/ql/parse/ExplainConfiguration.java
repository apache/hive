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

package org.apache.hadoop.hive.ql.parse;

import java.util.Map;

import org.apache.hadoop.fs.Path;

/**
 * ExplainConfiguration
 */

public class ExplainConfiguration {

  public enum VectorizationDetailLevel {

    SUMMARY(4), OPERATOR(3), EXPRESSION(2), DETAIL(1);

    public final int rank;
    VectorizationDetailLevel(int rank) {
      this.rank = rank;
    }
  };

  private boolean extended = false;
  private boolean formatted = false;
  private boolean dependency = false;
  private boolean logical = false;
  private boolean authorize = false;
  private boolean userLevelExplain = false;
  private boolean vectorization = false;
  private boolean vectorizationOnly = false;
  private VectorizationDetailLevel vectorizationDetailLevel = VectorizationDetailLevel.SUMMARY;
  private boolean locks = false;

  private Path explainRootPath;
  private Map<String, Long> opIdToRuntimeNumRows;

  public enum AnalyzeState {
    RUNNING, ANALYZING
  };

  private AnalyzeState analyze = null;

  public boolean isExtended() {
    return extended;
  }

  public void setExtended(boolean extended) {
    this.extended = extended;
  }

  public boolean isFormatted() {
    return formatted;
  }

  public void setFormatted(boolean formatted) {
    this.formatted = formatted;
  }

  public boolean isDependency() {
    return dependency;
  }

  public void setDependency(boolean dependency) {
    this.dependency = dependency;
  }

  public boolean isLogical() {
    return logical;
  }

  public void setLogical(boolean logical) {
    this.logical = logical;
  }

  public boolean isAuthorize() {
    return authorize;
  }

  public void setAuthorize(boolean authorize) {
    this.authorize = authorize;
  }

  public AnalyzeState getAnalyze() {
    return analyze;
  }

  public void setAnalyze(AnalyzeState analyze) {
    this.analyze = analyze;
  }

  public boolean isUserLevelExplain() {
    return userLevelExplain;
  }

  public void setUserLevelExplain(boolean userLevelExplain) {
    this.userLevelExplain = userLevelExplain;
  }

  public boolean isVectorization() {
    return vectorization;
  }

  public void setVectorization(boolean vectorization) {
    this.vectorization = vectorization;
  }

  public boolean isVectorizationOnly() {
    return vectorizationOnly;
  }

  public void setVectorizationOnly(boolean vectorizationOnly) {
    this.vectorizationOnly = vectorizationOnly;
  }

  public VectorizationDetailLevel getVectorizationDetailLevel() {
    return vectorizationDetailLevel;
  }

  public void setVectorizationDetailLevel(VectorizationDetailLevel vectorizationDetailLevel) {
    this.vectorizationDetailLevel = vectorizationDetailLevel;
  }

  public Path getExplainRootPath() {
    return explainRootPath;
  }

  public void setExplainRootPath(Path explainRootPath) {
    this.explainRootPath = explainRootPath;
  }

  public Map<String, Long> getOpIdToRuntimeNumRows() {
    return opIdToRuntimeNumRows;
  }

  public void setOpIdToRuntimeNumRows(Map<String, Long> opIdToRuntimeNumRows) {
    this.opIdToRuntimeNumRows = opIdToRuntimeNumRows;
  }

  public boolean isLocks() {
    return locks;
  }

  public void setLocks(boolean locks) {
    this.locks = locks;
  }
}
