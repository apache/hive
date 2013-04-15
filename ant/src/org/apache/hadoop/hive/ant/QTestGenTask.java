/**
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

package org.apache.hadoop.hive.ant;


import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.RuntimeConstants;

public class QTestGenTask extends Task {

  public class IncludeFilter implements FileFilter {

    Set<String> includeOnly;
    public IncludeFilter(Set<String> includeOnly) {
      this.includeOnly = includeOnly;
    }

    public boolean accept(File fpath) {
      return includeOnly == null || includeOnly.contains(fpath.getName());
    }
  }

  public class QFileFilter extends IncludeFilter {

    public QFileFilter(Set<String> includeOnly) {
      super(includeOnly);
    }

    public boolean accept(File fpath) {
      if (!super.accept(fpath)) {
        return false;
      }
      if (fpath.isDirectory() ||
          !fpath.getName().endsWith(".q")) {
        return false;
      }
      return true;
    }
    
  }
  
  public class DisabledQFileFilter extends IncludeFilter {
    public DisabledQFileFilter(Set<String> includeOnly) {
      super(includeOnly);
    }

    public boolean accept(File fpath) {
      if (!super.accept(fpath)) {
        return false;
      }
      return !fpath.isDirectory() && fpath.getName().endsWith(".q.disabled");
    }  
  }
  
  public class QFileRegexFilter extends QFileFilter {
    Pattern filterPattern;
    
    public QFileRegexFilter(String filter, Set<String> includeOnly) {
      super(includeOnly);
      filterPattern = Pattern.compile(filter);
    }
    
    public boolean accept(File filePath) {
      if (!super.accept(filePath)) {
        return false;
      }
      String testName = StringUtils.chomp(filePath.getName(), ".q");
      return filterPattern.matcher(testName).matches();
    }
  }

  private List<String> templatePaths = new ArrayList<String>();

  private String hiveRootDirectory;
  
  private String outputDirectory;
 
  private String queryDirectory;
 
  private String queryFile;

  private String includeQueryFile;

  private String excludeQueryFile;
  
  private String queryFileRegex;

  private String resultsDirectory;

  private String logDirectory;

  private String template;

  private String className;

  private String logFile;

  private String clusterMode;

  private String runDisabled;
  
  private String hadoopVersion;

  public void setHadoopVersion(String ver) {
    this.hadoopVersion = ver;
  }

  public String getHadoopVersion() {
    return hadoopVersion;
  }
  
  public void setClusterMode(String clusterMode) {
    this.clusterMode = clusterMode;
  }

  public String getClusterMode() {
    return clusterMode;
  }

  public void setRunDisabled(String runDisabled) {
    this.runDisabled = runDisabled;
  }

  public String getRunDisabled() {
    return runDisabled;
  }

  public void setLogFile(String logFile) {
    this.logFile = logFile;
  }

  public String getLogFile() {
    return logFile;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getClassName() {
    return className;
  }

  public void setTemplate(String template) {
    this.template = template;
  }

  public String getTemplate() {
    return template;
  }

  public void setHiveRootDirectory(File hiveRootDirectory) {
    try {
      this.hiveRootDirectory = hiveRootDirectory.getCanonicalPath();
    } catch (IOException ioe) {
      throw new BuildException(ioe);
    }
  }

  public String getHiveRootDirectory() {
    return hiveRootDirectory;
  }
  
  public void setTemplatePath(String templatePath) throws Exception {
    templatePaths.clear();
    for (String relativePath : templatePath.split(",")) {
      templatePaths.add(project.resolveFile(relativePath).getCanonicalPath());
    }
    System.out.println("Template Path:" + getTemplatePath());
  }

  public String getTemplatePath() {
    return StringUtils.join(templatePaths, ",");
  }

  public void setOutputDirectory(File outputDirectory) {
    try {
      this.outputDirectory = outputDirectory.getCanonicalPath();
    } catch (IOException ioe) {
      throw new BuildException(ioe);
    }
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public void setLogDirectory(String logDirectory) {
    this.logDirectory = logDirectory;
  }

  public String getLogDirectory() {
    return logDirectory;
  }

  public void setResultsDirectory(String resultsDirectory) {
    this.resultsDirectory = resultsDirectory;
  }

  public String getResultsDirectory() {
    return resultsDirectory;
  }

  public void setQueryDirectory(String queryDirectory) {
    this.queryDirectory = queryDirectory;
  }

  public String getQueryDirectory() {
    return queryDirectory;
  }

  public void setQueryFile(String queryFile) {
    this.queryFile = queryFile;
  }

  public String getQueryFile() {
    return queryFile;
  }

  public String getIncludeQueryFile() {
    return includeQueryFile;
  }

  public void setIncludeQueryFile(String includeQueryFile) {
    this.includeQueryFile = includeQueryFile;
  }

  public void setExcludeQueryFile(String excludeQueryFile) {
    this.excludeQueryFile = excludeQueryFile;
  }

  public String getExcludeQueryFile() {
    return excludeQueryFile;
  }

  public void setQueryFileRegex(String queryFileRegex) {
    this.queryFileRegex = queryFileRegex;
  }

  public String getQueryFileRegex() {
    return queryFileRegex;
  }

  public void execute() throws BuildException {

    if (getTemplatePath().equals("")) {
      throw new BuildException("No templatePath attribute specified");
    }

    if (template == null) {
      throw new BuildException("No template attribute specified");
    }

    if (outputDirectory == null) {
      throw new BuildException("No outputDirectory specified");
    }

    if (queryDirectory == null && queryFile == null ) {
      throw new BuildException("No queryDirectory or queryFile specified");
    }

    if (logDirectory == null) {
      throw new BuildException("No logDirectory specified");
    }

    if (resultsDirectory == null) {
      throw new BuildException("No resultsDirectory specified");
    }

    if (className == null) {
      throw new BuildException("No className specified");
    }

    Set<String> includeOnly = null;
    if (includeQueryFile != null && !includeQueryFile.isEmpty()) {
      includeOnly = new HashSet<String>(Arrays.asList(includeQueryFile.split(",")));
    }

    List<File> qFiles = new ArrayList<File>();
    HashMap<String, String> qFilesMap = new HashMap<String, String>();
    File hiveRootDir = null;
    File queryDir = null;
    File outDir = null;
    File resultsDir = null;
    File logDir = null;
    
    try {
      if (queryDirectory != null) {
        queryDir = new File(queryDirectory);
      }

      if (queryFile != null && !queryFile.equals("")) {
        // The user may have passed a list of files - comma seperated
        for (String qFile : queryFile.split(",")) {
          if (includeOnly != null && !includeOnly.contains(qFile)) {
            continue;
          }
          if (null != queryDir) {
            qFiles.add(new File(queryDir, qFile));
          } else {
            qFiles.add(new File(qFile));
          }
        }
      } else if (queryFileRegex != null && !queryFileRegex.equals("")) {
        qFiles.addAll(Arrays.asList(queryDir.listFiles(
            new QFileRegexFilter(queryFileRegex, includeOnly))));
      } else if (runDisabled != null && runDisabled.equals("true")) {
        qFiles.addAll(Arrays.asList(queryDir.listFiles(new DisabledQFileFilter(includeOnly))));
      } else {
        qFiles.addAll(Arrays.asList(queryDir.listFiles(new QFileFilter(includeOnly))));
      }

      if (excludeQueryFile != null && !excludeQueryFile.equals("")) {
        // Exclude specified query files, comma separated
        for (String qFile : excludeQueryFile.split(",")) {
          if (null != queryDir) {
            qFiles.remove(new File(queryDir, qFile));
          } else {
            qFiles.remove(new File(qFile));
          }
        }
      }

      hiveRootDir = new File(hiveRootDirectory);
      if (!hiveRootDir.exists()) {
        throw new BuildException("Hive Root Directory "
            + hiveRootDir.getCanonicalPath() + " does not exist");
      }
      
      Collections.sort(qFiles);
      for (File qFile : qFiles) {
        qFilesMap.put(qFile.getName(), getEscapedCanonicalPath(qFile));
      }

      // Make sure the output directory exists, if it doesn't
      // then create it.
      outDir = new File(outputDirectory);
      if (!outDir.exists()) {
        outDir.mkdirs();
      }

      logDir = new File(logDirectory);
      if (!logDir.exists()) {
        throw new BuildException("Log Directory " + logDir.getCanonicalPath() + " does not exist");
      }
      
      resultsDir = new File(resultsDirectory);
      if (!resultsDir.exists()) {
        throw new BuildException("Results Directory " + resultsDir.getCanonicalPath() + " does not exist");
      }
    } catch (Exception e) {
      throw new BuildException(e);
    }
    
    VelocityEngine ve = new VelocityEngine();

    try {
      ve.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_PATH, getTemplatePath());
      if (logFile != null) {
        File lf = new File(logFile);
        if (lf.exists()) {
          if (!lf.delete()) {
            throw new Exception("Could not delete log file " + lf.getCanonicalPath());
          }
        }

        ve.setProperty(RuntimeConstants.RUNTIME_LOG, logFile);
      }

      ve.init();
      Template t = ve.getTemplate(template);

      if (clusterMode == null) {
        clusterMode = new String("");
      }
      if (hadoopVersion == null) {
        hadoopVersion = "";
      }

      // For each of the qFiles generate the test
      VelocityContext ctx = new VelocityContext();
      ctx.put("className", className);
      ctx.put("hiveRootDir", getEscapedCanonicalPath(hiveRootDir));
      ctx.put("queryDir", getEscapedCanonicalPath(queryDir));
      ctx.put("qfiles", qFiles);
      ctx.put("qfilesMap", qFilesMap);
      ctx.put("resultsDir", getEscapedCanonicalPath(resultsDir));
      ctx.put("logDir", getEscapedCanonicalPath(logDir));
      ctx.put("clusterMode", clusterMode);
      ctx.put("hadoopVersion", hadoopVersion);

      File outFile = new File(outDir, className + ".java");
      FileWriter writer = new FileWriter(outFile);
      t.merge(ctx, writer);
      writer.close();

      System.out.println("Generated " + outFile.getCanonicalPath() + " from template " + template);
    } catch(BuildException e) {
      throw e;
    } catch(MethodInvocationException e) {
      throw new BuildException("Exception thrown by '" + e.getReferenceName() + "." +
                               e.getMethodName() +"'",
                               e.getWrappedThrowable());
    } catch(ParseErrorException e) {
      throw new BuildException("Velocity syntax error", e);
    } catch(ResourceNotFoundException e) {
      throw new BuildException("Resource not found", e);
    } catch(Exception e) {
      throw new BuildException("Generation failed", e);
    }
  }
  
  private static String getEscapedCanonicalPath(File file) throws IOException {
    if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
      // Escape the backward slash in CanonicalPath if the unit test runs on windows
      // e.g. dir.getCanonicalPath() gets the absolute path of local
      // directory. When we embed it directly in the generated java class it results
      // in compiler error in windows. Reason : the canonical path contains backward
      // slashes "C:\temp\etc\" and it is not a valid string in Java
      // unless we escape the backward slashes.
      return file.getCanonicalPath().replace("\\", "\\\\");
    }
    return file.getCanonicalPath();
  }
}
