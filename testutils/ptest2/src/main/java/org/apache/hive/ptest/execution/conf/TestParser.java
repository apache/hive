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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestParser {

  private final Context context;
  private final String testCasePropertyName;
  private final File sourceDirectory;
  private final Logger logger;

  public TestParser(Context context, String testCasePropertyName, 
      File sourceDirectory, Logger logger) {
    this.context = context;
    this.testCasePropertyName = testCasePropertyName;
    this.sourceDirectory = sourceDirectory;
    this.logger = logger;
  }
  private List<TestBatch> parseTests() {
    Splitter splitter = Splitter.on(" ").trimResults().omitEmptyStrings();
    Context unitContext = new Context(context.getSubProperties(
        Joiner.on(".").join("unitTests", "")));
    Set<String> excluded = Sets.newHashSet(splitter.split(unitContext.getString("exclude", "")));
    Set<String> isolated = Sets.newHashSet(splitter.split(unitContext.getString("isolate", "")));
    Set<String> included = Sets.newHashSet(splitter.split(unitContext.getString("include", "")));
    if(!included.isEmpty() && !excluded.isEmpty()) {
      throw new IllegalArgumentException(String.format("Included and excluded mutally exclusive." +
          " Included = %s, excluded = %s", included.toString(), excluded.toString()));
    }
    List<File> unitTestsDirs = Lists.newArrayList();
    for(String unitTestDir : Splitter.on(" ").omitEmptyStrings()
        .split(checkNotNull(unitContext.getString("directories"), "directories"))) {
      File unitTestParent = new File(sourceDirectory, unitTestDir);
      if(unitTestParent.isDirectory()) {
        unitTestsDirs.add(unitTestParent);
      } else {
        logger.warn("Unit test directory " + unitTestParent + " does not exist.");
      }
    }
    List<TestBatch> result = Lists.newArrayList();
    for(QFileTestBatch test : parseQFileTests()) {
      result.add(test);
      excluded.add(test.getDriver());
    }
    for(File unitTestDir : unitTestsDirs) {
      for(File classFile : FileUtils.listFiles(unitTestDir, new String[]{"class"}, true)) {
        String className = classFile.getName();
        logger.debug("In  " + unitTestDir  + ", found " + className);
        if(className.startsWith("Test") && !className.contains("$")) {
          String testName = className.replaceAll("\\.class$", "");
          if(excluded.contains(testName)) {
            logger.info("Exlcuding unit test " + testName);
          } else if(included.isEmpty() || included.contains(testName)) {
            if(isolated.contains(testName)) {
              logger.info("Executing isolated unit test " + testName);
              result.add(new UnitTestBatch(testCasePropertyName, testName, false));
            } else {
              logger.info("Executing parallel unit test " + testName);
              result.add(new UnitTestBatch(testCasePropertyName, testName, true));
            }
          }
        }
      }
    }
    return result;
  }
  private List<QFileTestBatch> parseQFileTests() {
    Splitter splitter = Splitter.on(" ").trimResults().omitEmptyStrings();
    List<QFileTestBatch> result = Lists.newArrayList();
    for(String alias : context.getString("qFileTests", "").split(" ")) {
      Context testContext = new Context(context.getSubProperties(
          Joiner.on(".").join("qFileTest", alias, "")));
      String driver = checkNotNull(testContext.getString("driver"), "driver").trim();
      // execute the driver locally?
      boolean isParallel = !testContext.getBoolean("isolateDriver", false);
      File directory = new File(sourceDirectory,
          checkNotNull(testContext.getString("directory"), "directory").trim());
      Set<String> excludedTests = Sets.newHashSet();
      for(String excludedTestGroup : splitter.split(testContext.getString("exclude", ""))) {
        excludedTests.addAll(Arrays.asList(testContext.
            getString(Joiner.on(".").join("groups", excludedTestGroup), "").trim().split(" ")));
      }
      Set<String> isolatedTests = Sets.newHashSet();
      for(String ioslatedTestGroup : splitter.split(testContext.getString("isolate", ""))) {
        isolatedTests.addAll(Arrays.asList(testContext.
            getString(Joiner.on(".").join("groups", ioslatedTestGroup), "").trim().split(" ")));
      }

      Set<String> includedTests = Sets.newHashSet();
      for(String includedTestGroup : splitter.split(testContext.getString("include", ""))) {
        includedTests.addAll(Arrays.asList(testContext.
            getString(Joiner.on(".").join("groups", includedTestGroup), "").trim().split(" ")));
      }
      if(!includedTests.isEmpty() && !excludedTests.isEmpty()) {
        throw new IllegalArgumentException(String.format("Included and excluded mutally exclusive." +
            " Included = %s, excluded = %s", includedTests.toString(), excludedTests.toString()));
      }
      result.addAll(createQFileTestBatches(
          driver,
          checkNotNull(testContext.getString("queryFilesProperty"), "queryFilesProperty").trim(),
          directory,
          testContext.getInteger("batchSize", 30),
          isParallel,
          excludedTests,
          includedTests,
          isolatedTests));
    }
    return result;
  }

  private List<QFileTestBatch> createQFileTestBatches(String driver, String queryFilesProperty,
      File directory, int batchSize, boolean isParallel, Set<String> excluded,
      Set<String> included, Set<String> isolated) {
    logger.info("Create batches for " + driver);
    List<String> qFileTestNames = Lists.newArrayList();
    for(File test : checkNotNull(directory.listFiles(), directory.getAbsolutePath())) {
      String testName = test.getName();
      if(test.isFile() &&
          testName.endsWith(".q") &&
          (included.isEmpty() || included.contains(testName))) {
        qFileTestNames.add(testName);
      }
    }
    List<QFileTestBatch> testBatches = Lists.newArrayList();
    List<String> testBatch = Lists.newArrayList();
    for(final String test : qFileTestNames) {
      if(excluded.contains(test)) {
        logger.info("Exlcuding test " + driver + " " + test);
      } else if(isolated.contains(test)) {
        logger.info("Executing isolated test " + driver + " " + test);
        testBatches.add(new QFileTestBatch(testCasePropertyName, driver, queryFilesProperty, Sets.newHashSet(test), isParallel));
      } else {
        if(testBatch.size() >= batchSize) {
          testBatches.add(new QFileTestBatch(testCasePropertyName, driver, queryFilesProperty, Sets.newHashSet(testBatch), isParallel));
          testBatch = Lists.newArrayList();
        }
        testBatch.add(test);
      }
    }
    if(!testBatch.isEmpty()) {
      testBatches.add(new QFileTestBatch(testCasePropertyName, driver, queryFilesProperty, Sets.newHashSet(testBatch), isParallel));
    }
    return testBatches;
  }


  public Supplier<List<TestBatch>> parse() {
    return new Supplier<List<TestBatch>>() {
      @Override
      public List<TestBatch> get() {
        return parseTests();
      }
    };
  }
}
