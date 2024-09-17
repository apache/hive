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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestMiniClusters.FsType;
import org.apache.hadoop.hive.ql.QTestMiniClusters.QTestSetup;

import java.util.HashMap;
import java.util.Map;

/**
 * QTestArguments composite used as arguments holder for QTestUtil initialization.
 */
public final class QTestArguments {

  private String outDir;
  private String logDir;
  private String confDir;
  private QTestMiniClusters.MiniClusterType clusterType;
  private String initScript;
  private String cleanupScript;
  private boolean withLlapIo;
  private FsType fsType;
  private QTestSetup qtestSetup;

  private Map<HiveConf.ConfVars,String> customConfigValueMap;

  private QTestArguments() {
  }

  public QTestMiniClusters.MiniClusterType getClusterType() {
    return clusterType;
  }

  private void setClusterType(QTestMiniClusters.MiniClusterType clusterType) {
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

  public FsType getFsType() {
    return fsType;
  }

  private void setFsType(QTestMiniClusters.FsType fsType) {
    this.fsType = fsType;
  }

  public QTestSetup getQTestSetup() {
    return qtestSetup;
  }

  private void setQTestSetup(QTestSetup qtestSetup) {
    this.qtestSetup = qtestSetup;
  }

  private void setCustomConfigValueMap(Map<HiveConf.ConfVars,String> customConfigValueMap){
    this.customConfigValueMap = customConfigValueMap;
  }

  public Map<HiveConf.ConfVars, String> getCustomConfs() {
    return this.customConfigValueMap;
  }

  /**
   * QTestArgumentsBuilder used for QTestArguments construction.
   */
  public static final class QTestArgumentsBuilder {

    private String outDir;
    private String logDir;
    private String confDir;
    private QTestMiniClusters.MiniClusterType clusterType;
    private String initScript;
    private String cleanupScript;
    private boolean withLlapIo;
    private FsType fsType;
    private QTestSetup qtestSetup;

    private Map<HiveConf.ConfVars, String> customConfigValueMap;

    private QTestArgumentsBuilder() {
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

    public QTestArgumentsBuilder withClusterType(QTestMiniClusters.MiniClusterType clusterType) {
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

    public QTestArgumentsBuilder withFsType(QTestMiniClusters.FsType fsType) {
      this.fsType = fsType;
      return this;
    }

    public QTestArgumentsBuilder withQTestSetup(QTestSetup qtestSetup) {
      this.qtestSetup = qtestSetup;
      return this;
    }

    public QTestArgumentsBuilder withCustomConfigValueMap(Map<HiveConf.ConfVars, String> customConfigValueMap) {
      this. customConfigValueMap =  customConfigValueMap;
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
          qtestSetup != null ? qtestSetup : new QTestSetup());

      testArguments.setCustomConfigValueMap(customConfigValueMap != null ? customConfigValueMap : new HashMap<>());
      return testArguments;
    }
  }

}
