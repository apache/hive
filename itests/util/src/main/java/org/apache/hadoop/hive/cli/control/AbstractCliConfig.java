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
package org.apache.hadoop.hive.cli.control;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestSystemProperties;
import org.apache.hadoop.hive.ql.QTestMiniClusters.FsType;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.apache.hive.testutils.HiveTestEnvSetup;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCliConfig {

  public static final String HIVE_ROOT = HiveTestEnvSetup.HIVE_ROOT;
  private static final Logger LOG = LoggerFactory.getLogger(AbstractCliConfig.class);

  private String queryFile;
  private String queryFileRegex;
  private String queryDirectory;
  // pending change to boolean
  private String runDisabled;
  // FIXME: file paths in strings should be changed to either File or Path ... anything but String
  private String resultsDirectory;
  private Set<String> excludedQueryFileNames = new LinkedHashSet<>();
  private String hadoopVersion;
  private String logDirectory;
  // these should have viable defaults
  private String cleanupScript;
  private String initScript;
  private String hiveConfDir;
  private MiniClusterType clusterType;
  private FsType fsType;
  private String metastoreType;

  // FIXME: null value is treated differently on the other end..when those filter will be
  // moved...this may change
  private Set<String> includeQueryFileNames;
  private Class<? extends CliAdapter> cliAdapter;
  private Map<HiveConf.ConfVars, String> customConfigValueMap;

  public AbstractCliConfig(Class<? extends CliAdapter> adapter) {
    cliAdapter = adapter;
    clusterType = MiniClusterType.NONE;
    queryFile = getSysPropValue("qfile");
    queryFileRegex = getSysPropValue("qfile_regex");
    runDisabled = getSysPropValue("run_disabled");
    // By default get metastoreType from system properties but allow specific configs to override
    metastoreType = QTestSystemProperties.getMetaStoreDb() == null ? "derby"
        : QTestSystemProperties.getMetaStoreDb();
  }

  protected void setQueryDir(String dir) {
    queryDirectory = getAbsolutePath(dir);
  }

  @Deprecated
  public void overrideUserQueryFile(String q) {
    queryFile = q;
  }

  public void includesFrom(URL resource, String key) {
    try (InputStream is = resource.openStream()) {
      Properties props = new Properties();
      props.load(is);
      String fileNames = getSysPropValue(key);
      if (fileNames == null) {
        fileNames = props.getProperty(key);
      }
      if (fileNames != null) {
        for (String qFile : TEST_SPLITTER.split(fileNames)) {
          includeQuery(qFile);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("error processing:" + resource, e);
    }
  }

  protected void includeQuery(String qFile) {
    if (includeQueryFileNames == null) {
      includeQueryFileNames = new HashSet<>();
    }
    includeQueryFileNames.add(qFile);
  }

  public void excludesFrom(URL resource, String key) {
    try (InputStream is = resource.openStream()) {
      Properties props = new Properties();
      props.load(is);

      String fileNames = getSysPropValue(key);
      if (fileNames == null) {
        fileNames = props.getProperty(key);
      }
      if (fileNames != null) {
        for (String qFile : TEST_SPLITTER.split(fileNames)) {
          excludeQuery(qFile);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("error processing:" + resource, e);
    }
  }

  private void excludeQuery(String qFile) {
    excludedQueryFileNames.add(qFile);
  }

  private static final Splitter TEST_SPLITTER =
      Splitter.onPattern("[, ]").trimResults().omitEmptyStrings();

  public static class IncludeFilter implements FileFilter {

    Set<String> includeOnly;

    public IncludeFilter(Set<String> includeOnly) {
      this.includeOnly = includeOnly;
    }

    @Override
    public boolean accept(File fpath) {
      return includeOnly == null || includeOnly.contains(fpath.getName());
    }
  }

  public static class QFileFilter extends IncludeFilter {

    public QFileFilter(Set<String> includeOnly) {
      super(includeOnly);
    }

    @Override
    public boolean accept(File fpath) {
      if (!super.accept(fpath)) {
        return false;
      }
      if (fpath.isDirectory() || !fpath.getName().endsWith(".q")) {
        return false;
      }
      return true;
    }
  }

  public static class DisabledQFileFilter extends IncludeFilter {
    public DisabledQFileFilter(Set<String> includeOnly) {
      super(includeOnly);
    }

    @Override
    public boolean accept(File fpath) {
      if (!super.accept(fpath)) {
        return false;
      }
      return !fpath.isDirectory() && fpath.getName().endsWith(".q.disabled");
    }
  }

  public static class QFileRegexFilter implements FileFilter {
    Pattern filterPattern;

    public QFileRegexFilter(String filter) {
      filterPattern = Pattern.compile(filter);
    }

    @Override
    public boolean accept(File filePath) {
      if (filePath.isDirectory() || !filePath.getName().endsWith(".q")) {
        return false;
      }
      String testName = StringUtils.chomp(filePath.getName(), ".q");
      return filterPattern.matcher(testName).matches();
    }
  }

  public Set<File> getQueryFiles() throws Exception {
    prepareDirs();

    Set<String> includeOnly = includeQueryFileNames;

    // queryDirectory should not be null
    File queryDir = new File(queryDirectory);

    // dedup file list
    Set<File> testFiles = new TreeSet<>();
    if (isQFileSpecified()) {
      // The user may have passed a list of files - comma separated
      for (String qFile : TEST_SPLITTER.split(queryFile)) {
        File qF;
        if (null != queryDir) {
          qF = new File(queryDir, qFile);
        } else {
          qF = new File(qFile);
        }
        if (excludedQueryFileNames.contains(qFile) && !isQFileSpecified()) {
          LOG.warn(qF.getAbsolutePath() + " is among the excluded query files for this driver."
              + " Please update CliConfigs.java or testconfiguration.properties file to"
              + " include the qfile or specify qfile through command line explicitly: -Dqfile=test.q");
        }
        testFiles.add(qF);
      }
    } else if (queryFileRegex != null && !queryFileRegex.equals("")) {
      for (String regex : TEST_SPLITTER.split(queryFileRegex)) {
        testFiles.addAll(Arrays.asList(queryDir.listFiles(new QFileRegexFilter(regex))));
      }
    } else if (runDisabled != null && runDisabled.equals("true")) {
      testFiles.addAll(Arrays.asList(queryDir.listFiles(new DisabledQFileFilter(includeOnly))));
    } else {
      testFiles.addAll(Arrays.asList(queryDir.listFiles(new QFileFilter(includeOnly))));
    }

    for (String qFileName : excludedQueryFileNames) {
      // in case of running as ptest, exclusions should be respected,
      // because test drivers receive every qfiles regardless of exclusions
      if ("hiveptest".equals(System.getProperty("user.name")) || !isQFileSpecified()
          || QTestSystemProperties.shouldForceExclusions()) {
        testFiles.remove(new File(queryDir, qFileName));
      }
    }

    return testFiles;
  }

  public boolean isQFileSpecified() {
    return queryFile != null && !queryFile.equals("");
  }

  private void prepareDirs() throws Exception {
    File hiveRootDir = new File(HIVE_ROOT);
    if (!hiveRootDir.exists()) {
      throw new RuntimeException(
          "Hive Root Directory " + hiveRootDir.getCanonicalPath() + " does not exist");
    }

    File logDir = new File(logDirectory);
    if (!logDir.exists()) {
      FileUtils.forceMkdir(logDir);
    }

    File resultsDir = new File(resultsDirectory);
    if (!resultsDir.exists()) {
      FileUtils.forceMkdir(resultsDir);
    }
  }

  public String getHadoopVersion() {
    if (hadoopVersion == null) {
      System.out.println("detecting hadoop.version from loaded libs");
      try {
        String hadoopPropsLoc = "/META-INF/maven/org.apache.hadoop/hadoop-hdfs/pom.properties";
        URL hadoopPropsURL = getClass().getResource(hadoopPropsLoc);
        if (hadoopPropsURL == null) {
          throw new RuntimeException("failed to get hadoop properties: " + hadoopPropsLoc);
        }
        try (InputStream is = hadoopPropsURL.openStream()) {
          Properties props = new Properties();
          props.load(is);
          hadoopVersion = props.getProperty("version");
          if (hadoopVersion == null) {
            throw new RuntimeException("version property not found");
          }
        } catch (IOException e) {
          throw new RuntimeException("unable to extract hadoop.version from: " + hadoopPropsURL, e);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "can't get hadoop.version ; specify manually using hadoop.version property!");
      }
    }
    return hadoopVersion;
  }

  protected void setHadoopVersion(String hadoopVersion) {
    this.hadoopVersion = hadoopVersion;
  }

  public String getLogDir() {
    return logDirectory;
  }

  protected void setLogDir(String logDirectory) {
    this.logDirectory = getAbsolutePath(logDirectory);
  }

  public String getResultsDir() {
    return resultsDirectory;
  }

  protected void setResultsDir(String resultsDir) {
    resultsDirectory = getAbsolutePath(resultsDir);
  }

  public String getCleanupScript() {
    return cleanupScript;
  }

  protected void setCleanupScript(String cleanupScript) {
    this.cleanupScript = cleanupScript;
  }

  public String getInitScript() {
    return initScript;
  }

  protected void setInitScript(String initScript) {
    String initScriptPropValue = getSysPropValue("initScript");
    if (initScriptPropValue != null) {
      System.out.println("initScript override(by system property):" + initScriptPropValue);
      this.initScript = initScriptPropValue;
    } else {
      this.initScript = initScript;
    }
  }
  public String getHiveConfDir() {
    return hiveConfDir;
  }

  protected void setHiveConfDir(String hiveConfDir) {
    if (hiveConfDir.trim().isEmpty()) {
      this.hiveConfDir = hiveConfDir;
    } else {
      this.hiveConfDir = getAbsolutePath(hiveConfDir);
    }
  }

  public MiniClusterType getClusterType() {
    return clusterType;
  }

  protected void setClusterType(MiniClusterType type) {
    String modeStr = getSysPropValue("clustermode");
    if (modeStr != null) {
      // FIXME: this should be changeto valueOf ...
      // that will also kill that fallback 'none' which is I think more like a problem than a
      // feature ;)
      clusterType = MiniClusterType.valueForString(modeStr);
    } else {
      clusterType = type;
    }
    if (clusterType == null) {
      throw new RuntimeException("clustertype cant be null");
    }
    this.setFsType(clusterType.getDefaultFsType());
  }

  protected FsType getFsType() {
    return this.fsType;
  }

  protected void setFsType(FsType fsType) {
    this.fsType = fsType;
  }

  private String getSysPropValue(String propName) {
    String propValue = System.getProperty(propName);
    if (propValue == null || propValue.trim().length() == 0) {
      return null;
    }
    System.out.println("property: " + propName + " used as override with val: " + propValue);
    return propValue.trim();
  }

  public CliAdapter getCliAdapter() {
    try {
      Constructor<? extends CliAdapter> cz = cliAdapter.getConstructor(AbstractCliConfig.class);
      return cz.newInstance(this);
    } catch (Exception e) {
      throw new RuntimeException("unable to build adapter", e);
    }
  }

  public String getQueryDirectory() {
    return queryDirectory;
  }

  private String getAbsolutePath(String dir) {
    return new File(new File(HIVE_ROOT), dir).getAbsolutePath();
  }

  public String getMetastoreType() {
    return metastoreType;
  }

  protected void setMetastoreType(String metastoreType) {
    this.metastoreType = metastoreType;
  }

  protected void setCustomConfigValueMap(Map<HiveConf.ConfVars, String> customConfigValueMap) {
    this.customConfigValueMap = customConfigValueMap;
  }
  
  public Map<HiveConf.ConfVars, String> getCustomConfigValueMap() {
    return this.customConfigValueMap;
  }
}
