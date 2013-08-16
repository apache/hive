/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public class TestConfiguration {
  private static final String REPOSITORY_TYPE = "repositoryType";
  private static final String GIT = "git";
  private static final String SVN = "svn";
  private static final String REPOSITORY = "repository";
  private static final String REPOSITORY_NAME = "repositoryName";
  private static final String BRANCH = "branch";
  private static final String ANT_ARGS = "antArgs";
  private static final String ANT_ENV_OPTS = "antEnvOpts";
  private static final String JAVA_HOME = "javaHome";
  private static final String JIRA_URL = "jiraUrl";
  private static final String JIRA_USER = "jiraUser";
  private static final String JIRA_PASSWORD = "jiraPassword";
  public static final String JENKINS_URL = "jenkinsURL";

  private final Context context;
  private String antArgs;
  private String antEnvOpts;
  private String repositoryType;
  private String repository;
  private String repositoryName;
  private String patch;
  private String javaHome;
  private String branch;
  private final String jenkinsURL;
  private final String jiraUrl;
  private final String jiraUser;
  private final String jiraPassword;
  private String jiraName;
  private boolean clearLibraryCache;

  @VisibleForTesting
  public TestConfiguration(Context context, Logger logger)
      throws IOException {
    this.context = context;
    repositoryType = context.getString(REPOSITORY_TYPE, GIT).trim();
    repository =  Preconditions.checkNotNull(context.getString(REPOSITORY), REPOSITORY).trim();
    repositoryName =  Preconditions.checkNotNull(context.getString(REPOSITORY_NAME), REPOSITORY_NAME).trim();
    if(GIT.equals(repositoryType)) {
      branch =  Preconditions.checkNotNull(context.getString(BRANCH), BRANCH).trim();
    } else if(SVN.equals(repositoryType)) {
      branch = Strings.nullToEmpty(null);
    } else {
      throw new IllegalArgumentException("Unkown repository type '" + repositoryType + "'");
    }
    antArgs =  Preconditions.checkNotNull(context.getString(ANT_ARGS), ANT_ARGS).trim();
    antEnvOpts =  context.getString(ANT_ENV_OPTS, "").trim();
    javaHome =  context.getString(JAVA_HOME, "").trim();
    patch = Strings.nullToEmpty(null);
    jiraName = Strings.nullToEmpty(null);
    jiraUrl = context.getString(JIRA_URL, "").trim();
    jiraUser = context.getString(JIRA_USER, "").trim();
    jiraPassword = context.getString(JIRA_PASSWORD, "").trim();
    jenkinsURL = context.getString(JENKINS_URL, "https://builds.apache.org/job").trim();

  }
  public Context getContext() {
    return context;
  }
  public String getJenkinsURL() {
    return jenkinsURL;
  }
  public String getJiraName() {
    return jiraName;
  }
  public void setJiraName(String jiraName) {
    this.jiraName = Strings.nullToEmpty(jiraName);
  }
  public boolean isClearLibraryCache() {
    return clearLibraryCache;
  }
  public void setClearLibraryCache(boolean clearLibraryCache) {
    this.clearLibraryCache = clearLibraryCache;
  }
  public String getJiraUrl() {
    return jiraUrl;
  }
  public String getJiraUser() {
    return jiraUser;
  }
  public String getJiraPassword() {
    return jiraPassword;
  }
  public String getRepositoryType() {
    return repositoryType;
  }
  public String getRepositoryName() {
    return repositoryName;
  }
  public String getRepository() {
    return repository;
  }
  public String getBranch() {
    return branch;
  }
  public String getAntArgs() {
    return antArgs;
  }
  public String getJavaHome() {
    return javaHome;
  }
  public String getPatch() {
    return patch;
  }
  public void setPatch(String patch) {
    this.patch = Strings.nullToEmpty(patch);
  }
  public void setRepository(String repository) {
    this.repository = Strings.nullToEmpty(repository);
  }
  public void setRepositoryName(String repositoryName) {
    this.repositoryName = Strings.nullToEmpty(repositoryName);
  }
  public void setBranch(String branch) {
    this.branch = Strings.nullToEmpty(branch);
  }
  public void setJavaHome(String javaHome) {
    this.javaHome = Strings.nullToEmpty(javaHome);
  }
  public void setAntArgs(String antArgs) {
    this.antArgs = Strings.nullToEmpty(antArgs);
  }
  public String getAntEnvOpts() {
    return antEnvOpts;
  }
  public void setAntEnvOpts(String antEnvOpts) {
    this.antEnvOpts = Strings.nullToEmpty(antEnvOpts);
  }
  @Override
  public String toString() {
    return "Configuration [context=" + context + ", antArgs=" + antArgs
        + ", antEnvOpts=" + antEnvOpts + ", repository=" + repository
        + ", repositoryName=" + repositoryName + ", patch=" + patch
        + ", javaHome=" + javaHome + ", branch=" + branch + "]";
  }
  public static TestConfiguration fromInputStream(InputStream inputStream, Logger logger)
      throws IOException {
    Properties properties = new Properties();
    properties.load(inputStream);
    Context context = new Context(Maps.fromProperties(properties));
    return new TestConfiguration(context, logger);
  }
  public static TestConfiguration fromFile(String file, Logger logger) throws IOException {
    return fromFile(new File(file), logger);
  }
  public static TestConfiguration fromFile(File file, Logger logger) throws IOException {
    InputStream in = new FileInputStream(file);
    try {
      return fromInputStream(in, logger);
    } finally {
      in.close();
    }
  }
}
