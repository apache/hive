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

/**
 * QTestArguments composite used as arguments holder for QTestUtil initialization.
 */
public final class QTestArguments {

  private String outDir;
  private String logDir;
  private String confDir;
  private QTestUtil.MiniClusterType clusterType;
  private String initScript;
  private String cleanupScript;
  private boolean withLlapIo;
  private QTestUtil.FsType fsType;
  private QTestUtil.QTestSetup qtestSetup;

  private QTestArguments() {
  }

  public QTestUtil.MiniClusterType getClusterType() {
    return clusterType;
  }

  private void setClusterType(QTestUtil.MiniClusterType clusterType) {
    this.clusterType = clusterType;
  }

  public String getOutDir() {
    return outDir;
  }

  private void setOutDir(String outDir) {
    this.outDir = outDir;
  }

  public String getLogDir() {
    return logDir;
  }

  private void setLogDir(String logDir) {
    this.logDir = logDir;
  }

  public String getConfDir() {
    return confDir;
  }

  private void setConfDir(String confDir) {
    this.confDir = confDir;
  }

  private void setInitScript(String initScript) {
    this.initScript = initScript;
  }

  public String getInitScript() {
    return initScript;
  }

  private void setCleanupScript(String cleanupScript) {
    this.cleanupScript = cleanupScript;
  }

  public String getCleanupScript() {
    return cleanupScript;
  }

  public boolean isWithLlapIo() {
    return withLlapIo;
  }

  private void setWithLlapIo(boolean withLlapIo) {
    this.withLlapIo = withLlapIo;
  }

  public QTestUtil.FsType getFsType() {
    return fsType;
  }

  private void setFsType(QTestUtil.FsType fsType) {
    this.fsType = fsType;
  }

  public QTestUtil.QTestSetup getQTestSetup() {
    return qtestSetup;
  }

  private void setQTestSetup(QTestUtil.QTestSetup qtestSetup) {
    this.qtestSetup = qtestSetup;
  }

  /**
   * QTestArgumentsBuilder used for QTestArguments construction.
   */
  public static final class QTestArgumentsBuilder {

    private String outDir;
    private String logDir;
    private String confDir;
    private QTestUtil.MiniClusterType clusterType;
    private String initScript;
    private String cleanupScript;
    private boolean withLlapIo;
    private QTestUtil.FsType fsType;
    private QTestUtil.QTestSetup qtestSetup;

    private QTestArgumentsBuilder(){
    }

    public static QTestArgumentsBuilder instance() {
      return new QTestArgumentsBuilder();
    }

    public QTestArgumentsBuilder withOutDir(String outDir) {
      this.outDir = outDir;
      return this;
    }

    public QTestArgumentsBuilder withLogDir(String logDir) {
      this.logDir = logDir;
      return this;
    }

    public QTestArgumentsBuilder withConfDir(String confDir) {
      this.confDir = confDir;
      return this;
    }

    public QTestArgumentsBuilder withClusterType(QTestUtil.MiniClusterType clusterType) {
      this.clusterType = clusterType;
      return this;
    }

    public QTestArgumentsBuilder withInitScript(String initScript) {
      this.initScript = initScript;
      return this;
    }

    public QTestArgumentsBuilder withCleanupScript(String cleanupScript) {
      this.cleanupScript = cleanupScript;
      return this;
    }

    public QTestArgumentsBuilder withLlapIo(boolean withLlapIo) {
      this.withLlapIo = withLlapIo;
      return this;
    }

    public QTestArgumentsBuilder withFsType(QTestUtil.FsType fsType) {
      this.fsType = fsType;
      return this;
    }

    public QTestArgumentsBuilder withQTestSetup(QTestUtil.QTestSetup qtestSetup) {
      this.qtestSetup = qtestSetup;
      return this;
    }

    public QTestArguments build() {
      QTestArguments testArguments = new QTestArguments();
      testArguments.setOutDir(outDir);
      testArguments.setLogDir(logDir);
      testArguments.setConfDir(confDir);
      testArguments.setClusterType(clusterType);
      testArguments.setInitScript(initScript);
      testArguments.setCleanupScript(cleanupScript);
      testArguments.setWithLlapIo(withLlapIo);

      testArguments.setFsType(
          fsType != null ? fsType : clusterType.getDefaultFsType());

      testArguments.setQTestSetup(
          qtestSetup != null ? qtestSetup : new QTestUtil.QTestSetup());

      return testArguments;
    }
  }

}
