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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestParser {

  private static final Splitter TEST_SPLITTER = Splitter.onPattern("[, ]")
      .trimResults().omitEmptyStrings();

  private static final String QTEST_MODULE_NAME = "itests/qtest";
  private static final String QTEST_SPARK_MODULE_NAME = "itests/qtest-spark";

  private final AtomicInteger batchIdCounter;

  private final Context context;
  private final String testCasePropertyName;
  private final File sourceDirectory;
  private final Logger logger;

  public TestParser(Context context, AtomicInteger batchIdCounter, String testCasePropertyName,
      File sourceDirectory, Logger logger) {
    this.context = context;
    this.batchIdCounter = batchIdCounter;
    this.testCasePropertyName = testCasePropertyName;
    this.sourceDirectory = sourceDirectory;
    this.logger = logger;
  }
  private List<TestBatch> parseTests() {

    Set<String> excluded = new HashSet<String>();


    List<TestBatch> result = Lists.newArrayList();
    for(QFileTestBatch test : parseQFileTests()) {
      result.add(test);
      excluded.add(test.getDriver());
    }

    Collection<TestBatch> unitTestBatches =
        new UnitTestPropertiesParser(context, batchIdCounter, testCasePropertyName, sourceDirectory, logger,
            excluded).generateTestBatches();
    result.addAll(unitTestBatches);

    return result;
  }
  private List<QFileTestBatch> parseQFileTests() {
    Map<String, Properties> properties = parseQTestProperties();

    List<QFileTestBatch> result = Lists.newArrayList();
    String qFileTestsString = context.getString("qFileTests",null);
    String []aliases;
    if (qFileTestsString != null) {
      aliases = qFileTestsString.split(" ");
    } else {
      aliases = new String[0];
    }

    for(String alias : aliases) {
      Context testContext = new Context(context.getSubProperties(
          Joiner.on(".").join("qFileTest", alias, "")));
      String driver = checkNotNull(testContext.getString("driver"), "driver").trim();
      // execute the driver locally?
      boolean isParallel = !testContext.getBoolean("isolateDriver", false);
      File directory = new File(sourceDirectory,
          checkNotNull(testContext.getString("directory"), "directory").trim());
      Set<String> excludedTests = Sets.newHashSet();
      for(String excludedTestGroup : TEST_SPLITTER.split(testContext.getString("exclude", ""))) {
        excludedTests.addAll(Arrays.asList(testContext.
            getString(Joiner.on(".").join("groups", excludedTestGroup), "").trim().split(" ")));
        expandTestProperties(excludedTests, properties);
      }
      Set<String> isolatedTests = Sets.newHashSet();
      for(String ioslatedTestGroup : TEST_SPLITTER.split(testContext.getString("isolate", ""))) {
        isolatedTests.addAll(Arrays.asList(testContext.
            getString(Joiner.on(".").join("groups", ioslatedTestGroup), "").trim().split(" ")));
        expandTestProperties(isolatedTests, properties);
      }

      Set<String> includedTests = Sets.newHashSet();
      for(String includedTestGroup : TEST_SPLITTER.split(testContext.getString("include", ""))) {
        includedTests.addAll(Arrays.asList(testContext.
            getString(Joiner.on(".").join("groups", includedTestGroup), "").trim().split(" ")));
        expandTestProperties(includedTests, properties);
      }

      //excluded overrides included
      includedTests.removeAll(excludedTests);

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
        testBatches.add(new QFileTestBatch(batchIdCounter, testCasePropertyName, driver, queryFilesProperty,
            Sets.newHashSet(test), isParallel, getModuleName(driver)));
      } else {
        if(testBatch.size() >= batchSize) {
          testBatches.add(new QFileTestBatch(batchIdCounter, testCasePropertyName, driver, queryFilesProperty,
              Sets.newHashSet(testBatch), isParallel, getModuleName(driver)));
          testBatch = Lists.newArrayList();
        }
        testBatch.add(test);
      }
    }
    if(!testBatch.isEmpty()) {
      testBatches.add(new QFileTestBatch(batchIdCounter, testCasePropertyName, driver, queryFilesProperty,
          Sets.newHashSet(testBatch), isParallel, getModuleName(driver)));
    }
    return testBatches;
  }

  /**
   * @return properties loaded from files specified in qFileTests.propertyFiles.${fileName}=${filePath}
   */
  private Map<String, Properties> parseQTestProperties() {
    Map<String, String> propFiles = context.getSubProperties("qFileTests.propertyFiles.");
    Map<String, Properties> propertyMap = new HashMap<String, Properties>();
    for (String propFile : propFiles.keySet()) {
      Properties properties = new Properties();
      String path = sourceDirectory + File.separator + propFiles.get(propFile);
      FileInputStream fis = null;
      try {
        fis = new FileInputStream(path);
        properties.load(fis);
      } catch (IOException e) {
        logger.warn("Error processing Qtest property file", e);
        throw new IllegalArgumentException("Error processing Qtest property file: " + path);
      } finally {
        try {
          if (fis != null) {
            fis.close();
          }
        } catch (IOException e) { //ignore
        }
      }
      propertyMap.put(propFile, properties);
      logger.info("Loaded Qtest property file: " + path);
    }
    return propertyMap;
  }

  /**
   * If any of given tests are of the form: ${fileName}.${property} (test list within a property file),
   * then expand them.  Then remove those markers from the list of tests.
   */
  private void expandTestProperties(Set<String> tests, Map<String, Properties> propMap) {
    Set<String> toRemove = new HashSet<String>();
    Set<String> toAdd = new HashSet<String>();

    String pattern = "([^\\.]*)\\.\\$\\{([^}]*)}";
    Pattern r = Pattern.compile(pattern);
    for (String test : tests) {
      Matcher m = r.matcher(test);
      if (m.find()) {
        toRemove.add(test);
        logger.info("Expanding qfile property: " + test);
        String propName = m.group(1);
        String propValue = m.group(2);
        Properties props = propMap.get(propName);
        if (props == null) {
          logger.warn("No properties found for : " + propName);
          throw new IllegalArgumentException("No properties found for : " + propName);
        }
        String result = (String) props.get(propValue);
        if (result == null || result.isEmpty()) {
          logger.warn("No properties found in file: " + propName + " for property: " + propValue);
          throw new IllegalArgumentException("No propertifies found in file: " + propName + " for property: " + propValue);
        }
        Iterable<String> splits = TEST_SPLITTER.split(result);
        for (String split : splits) {
          toAdd.add(split);
        }
      }
    }
    tests.removeAll(toRemove);
    tests.addAll(toAdd);
  }

  private String getModuleName(String driverName) {
    if (driverName.toLowerCase().contains("spark")) {
      return QTEST_SPARK_MODULE_NAME;
    } else {
      return QTEST_MODULE_NAME;
    }
  }

  public Supplier<List<TestBatch>> parse() {
    return new Supplier<List<TestBatch>>() {
      @Override
      public List<TestBatch> get() {
        return parseTests();
      }
    };
  }

  /**
   * Manually test this against any property file.
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      throw new IllegalArgumentException("Enter the property file location");
    }
    Logger log = LoggerFactory
        .getLogger(TestParser.class);
    File workingDir = new File("../..");
    File testConfigurationFile = new File(args[0]);
    final Context ctx = Context.fromFile(testConfigurationFile);
    TestConfiguration conf = TestConfiguration.withContext(ctx, log);
    TestParser testParser = new TestParser(conf.getContext(), new AtomicInteger(1), "test", workingDir, log);
    List<TestBatch> testBatches = testParser.parse().get();
    for (TestBatch testBatch : testBatches) {
      System.out.println(testBatch.getTestArguments());
    }
  }
}
