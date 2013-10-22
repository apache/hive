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
package org.apache.hive.ptest.execution;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.approvaltests.Approvals;
import org.approvaltests.reporters.JunitReporter;
import org.approvaltests.reporters.UseReporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;

@UseReporter(JunitReporter.class)
public class TestScripts  {

  private File baseDir;
  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
  }

  @After
  public void teardown() throws Exception {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }
  @Test
  public void testBatch() throws Throwable {
    Map<String, String> templateVariables = Maps.newHashMap();
    templateVariables.put("repository", "git:///repo1");
    templateVariables.put("repositoryName", "apache");
    templateVariables.put("branch", "branch-1");
    templateVariables.put("localDir", "/some/local/dir");
    templateVariables.put("workingDir", "/some/working/dir");
    templateVariables.put("buildTool", "maven");
    templateVariables.put("antArgs", "-Dant=arg1");
    templateVariables.put("testClass", "TestCliDriver");
    templateVariables.put("buildTag", "build-1");
    templateVariables.put("logDir", "/some/log/dir");
    templateVariables.put("instanceName", "instance-1");
    templateVariables.put("batchName","batch-1");
    templateVariables.put("numOfFailedTests", "20");
    templateVariables.put("maxSourceDirs", String.valueOf(5));
    templateVariables.put("testArguments", "-Dtest=arg1");
    templateVariables.put("clearLibraryCache", "true");
    templateVariables.put("javaHome", "/usr/java/jdk1.7");
    templateVariables.put("antEnvOpts", "-Dhttp.proxyHost=somehost -Dhttp.proxyPort=3128");
    templateVariables.put("antTestArgs", "-DgrammarBuild.notRequired=true -Dskip.javadoc=true");
    templateVariables.put("antTestTarget", "testonly");
    String template = readResource("batch-exec.vm");
    String actual = getTemplateResult(template, templateVariables);
    Approvals.verify(actual);
  }
  @Test
  public void testAlternativeTestJVM() throws Throwable {
    Map<String, String> templateVariables = Maps.newHashMap();
    templateVariables.put("repository", "git:///repo1");
    templateVariables.put("repositoryName", "apache");
    templateVariables.put("branch", "branch-1");
    templateVariables.put("localDir", "/some/local/dir");
    templateVariables.put("workingDir", "/some/working/dir");
    templateVariables.put("buildTool", "ant");
    templateVariables.put("testClass", "TestCliDriver");
    templateVariables.put("antArgs", "-Dant=arg1");
    templateVariables.put("buildTag", "build-1");
    templateVariables.put("logDir", "/some/log/dir");
    templateVariables.put("instanceName", "instance-1");
    templateVariables.put("batchName","batch-1");
    templateVariables.put("numOfFailedTests", "20");
    templateVariables.put("maxSourceDirs", String.valueOf(5));
    templateVariables.put("testArguments", "-Dtest=arg1");
    templateVariables.put("clearLibraryCache", "true");
    templateVariables.put("javaHome", "/usr/java/jdk1.7");
    templateVariables.put("javaHomeForTests", "/usr/java/jdk1.7-other");
    templateVariables.put("antEnvOpts", "-Dhttp.proxyHost=somehost -Dhttp.proxyPort=3128");
    templateVariables.put("antTestArgs", "");
    String template = readResource("batch-exec.vm");
    String actual = getTemplateResult(template, templateVariables);
    Approvals.verify(actual);
  }
  @Test
  public void testPrepNone() throws Throwable {
    Map<String, String> templateVariables = Maps.newHashMap();
    templateVariables.put("repository", "git:///repo1");
    templateVariables.put("repositoryName", "apache");
    templateVariables.put("branch", "branch-1");
    templateVariables.put("localDir", "/some/local/dir");
    templateVariables.put("workingDir", "/some/working/dir");
    templateVariables.put("buildTool", "ant");
    templateVariables.put("antArgs", "-Dant=arg1");
    templateVariables.put("buildTag", "build-1");
    templateVariables.put("logDir", "/some/log/dir");
    templateVariables.put("testArguments", "-Dtest=arg1");
    templateVariables.put("clearLibraryCache", "false");
    templateVariables.put("javaHome", "/usr/java/jdk1.7");
    templateVariables.put("antEnvOpts", "-Dhttp.proxyHost=somehost -Dhttp.proxyPort=3128");
    String template = readResource("source-prep.vm");
    String actual = getTemplateResult(template, templateVariables);
    Approvals.verify(actual);
  }
  @Test
  public void testPrepGit() throws Throwable {
    Map<String, String> templateVariables = Maps.newHashMap();
    templateVariables.put("repository", "git:///repo1");
    templateVariables.put("repositoryName", "apache");
    templateVariables.put("branch", "branch-1");
    templateVariables.put("localDir", "/some/local/dir");
    templateVariables.put("workingDir", "/some/working/dir");
    templateVariables.put("antArgs", "-Dant=arg1");
    templateVariables.put("buildTag", "build-1");
    templateVariables.put("logDir", "/some/log/dir");
    templateVariables.put("testArguments", "-Dtest=arg1");
    templateVariables.put("clearLibraryCache", "true");
    templateVariables.put("javaHome", "/usr/java/jdk1.7");
    templateVariables.put("antEnvOpts", "-Dhttp.proxyHost=somehost -Dhttp.proxyPort=3128");
    templateVariables.put("repositoryType", "git");
    String template = readResource("source-prep.vm");
    String actual = getTemplateResult(template, templateVariables);
    Approvals.verify(actual);
  }
  @Test
  public void testPrepSvn() throws Throwable {
    Map<String, String> templateVariables = Maps.newHashMap();
    templateVariables.put("repository", "https://svn.apache.org/repos/asf/hive/trunk");
    templateVariables.put("repositoryName", "apache");
    templateVariables.put("branch", "");
    templateVariables.put("localDir", "/some/local/dir");
    templateVariables.put("workingDir", "/some/working/dir");
    templateVariables.put("buildTool", "maven");
    templateVariables.put("antArgs", "-Dant=arg1");
    templateVariables.put("buildTag", "build-1");
    templateVariables.put("logDir", "/some/log/dir");
    templateVariables.put("testArguments", "-Dtest=arg1");
    templateVariables.put("clearLibraryCache", "true");
    templateVariables.put("javaHome", "/usr/java/jdk1.7");
    templateVariables.put("antEnvOpts", "-Dhttp.proxyHost=somehost -Dhttp.proxyPort=3128");
    templateVariables.put("repositoryType", "svn");
    String template = readResource("source-prep.vm");
    String actual = getTemplateResult(template, templateVariables);
    Approvals.verify(actual);
  }

  protected static String readResource(String resource) throws IOException {
    return Resources.toString(Resources.getResource(resource), Charsets.UTF_8);
  }
  protected static String getTemplateResult(String command, Map<String, String> keyValues)
      throws IOException {
    VelocityContext context = new VelocityContext();
    for(String key : keyValues.keySet()) {
      context.put(key, keyValues.get(key));
    }
    StringWriter writer = new StringWriter();
    if(!Velocity.evaluate(context, writer, command, command)) {
      throw new IOException("Unable to render " + command + " with " + keyValues);
    }
    writer.close();
    return writer.toString();
  }
}
