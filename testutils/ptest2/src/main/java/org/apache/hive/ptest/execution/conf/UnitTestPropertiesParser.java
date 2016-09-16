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
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

class UnitTestPropertiesParser {

  private static final Splitter VALUE_SPLITTER = Splitter.onPattern("[, ]")
      .trimResults().omitEmptyStrings();

  // Prefix for top level properties.
  static final String PROP_PREFIX_ROOT = "unitTests";
  // Prefix used to specify module specific properties. Mainly to avoid conflicts with older unitTests properties
  static final String PROP_PREFIX_MODULE = "ut";

  static final String PROP_DIRECTORIES = "directories";
  static final String PROP_INCLUDE = "include";
  static final String PROP_EXCLUDE = "exclude";
  static final String PROP_ISOLATE = "isolate";
  static final String PROP_SKIP_BATCHING = "skipBatching";
  static final String PROP_BATCH_SIZE = "batchSize";
  static final String PROP_SUBDIR_FOR_PREFIX = "subdirForPrefix";

  static final String PROP_ONE_MODULE = "module";
  static final String PROP_MODULE_LIST = "modules";

  private final AtomicInteger batchIdCounter;

  static final int DEFAULT_PROP_BATCH_SIZE = 1;
  static final int DEFAULT_PROP_BATCH_SIZE_NOT_SPECIFIED = -1;
  static final int DEFAULT_PROP_BATCH_SIZE_INCLUDE_ALL = 0;
  static final String DEFAULT_PROP_DIRECTORIES = ".";
  static final String DEFAULT_PROP_SUBDIR_FOR_PREFIX = "target";

  static final String MODULE_NAME_TOP_LEVEL = "_root_"; // Special module for tests in the rootDir.
  static final String PREFIX_TOP_LEVEL = ".";

  private final Context unitRootContext; // Everything prefixed by ^unitTests.
  private final Context unitModuleContext; // Everything prefixed by ^ut.
  private final String testCasePropertyName;
  private final Logger logger;
  private final File sourceDirectory;
  private final FileListProvider fileListProvider;
  private final Set<String> excludedProvided; // excludedProvidedBy Framework vs excludedConfigured
  private final boolean inTest;


  @VisibleForTesting
  UnitTestPropertiesParser(Context testContext, AtomicInteger batchIdCounter, String testCasePropertyName,
                           File sourceDirectory, Logger logger,
                           FileListProvider fileListProvider,
                           Set<String> excludedProvided, boolean inTest) {
    logger.info("{} created with sourceDirectory={}, testCasePropertyName={}, excludedProvide={}",
        "fileListProvider={}, inTest={}",
        UnitTestPropertiesParser.class.getSimpleName(), sourceDirectory, testCasePropertyName,
        excludedProvided,
        (fileListProvider == null ? "null" : fileListProvider.getClass().getSimpleName()), inTest);
    Preconditions.checkNotNull(batchIdCounter, "batchIdCounter cannot be null");
    Preconditions.checkNotNull(testContext, "testContext cannot be null");
    Preconditions.checkNotNull(testCasePropertyName, "testCasePropertyName cannot be null");
    Preconditions.checkNotNull(sourceDirectory, "sourceDirectory cannot be null");
    Preconditions.checkNotNull(logger, "logger must be specified");
    this.batchIdCounter = batchIdCounter;
    this.unitRootContext =
        new Context(testContext.getSubProperties(Joiner.on(".").join(PROP_PREFIX_ROOT, "")));
    this.unitModuleContext =
        new Context(testContext.getSubProperties(Joiner.on(".").join(PROP_PREFIX_MODULE, "")));
    this.sourceDirectory = sourceDirectory;
    this.testCasePropertyName = testCasePropertyName;
    this.logger = logger;
    if (excludedProvided != null) {
      this.excludedProvided = excludedProvided;
    } else {
      this.excludedProvided = new HashSet<>();
    }
    if (fileListProvider != null) {
      this.fileListProvider = fileListProvider;
    } else {
      this.fileListProvider = new DefaultFileListProvider();
    }
    this.inTest = inTest;

  }

  UnitTestPropertiesParser(Context testContext, AtomicInteger batchIdCounter, String testCasePropertyName,
                           File sourceDirectory, Logger logger,
                           Set<String> excludedProvided) {
    this(testContext, batchIdCounter, testCasePropertyName, sourceDirectory, logger, null, excludedProvided, false);
  }


  Collection<TestBatch> generateTestBatches() {
    try {
      return parse();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  private Collection<TestBatch> parse() throws IOException {

    RootConfig rootConfig = getRootConfig(unitRootContext);
    logger.info("RootConfig: " + rootConfig);

    // TODO: Set this up as a tree, instead of a flat list.
    Map<String, ModuleConfig> moduleConfigs = extractModuleConfigs();
    logger.info("ModuleConfigs: {} ", moduleConfigs);

    List<TestDir> unitTestsDirs = processPropertyDirectories();

    validateConfigs(rootConfig, moduleConfigs, unitTestsDirs);

    LinkedHashMap<String, LinkedHashSet<TestInfo>> allTests =
        generateFullTestSet(rootConfig, moduleConfigs, unitTestsDirs);


    return createTestBatches(allTests, rootConfig, moduleConfigs);
  }

  private Collection<TestBatch> createTestBatches(
      LinkedHashMap<String, LinkedHashSet<TestInfo>> allTests, RootConfig rootConfig,
      Map<String, ModuleConfig> moduleConfigs) {
    List<TestBatch> testBatches = new LinkedList<>();
    for (Map.Entry<String, LinkedHashSet<TestInfo>> entry : allTests.entrySet()) {
      logger.info("Creating test batches for module={}, numTests={}", entry.getKey(),
          entry.getValue().size());
      String currentModule = entry.getKey();
      String currentPathPrefix = getPathPrefixFromModuleName(currentModule);
      int batchSize = rootConfig.batchSize;
      if (moduleConfigs.containsKey(currentModule)) {
        ModuleConfig moduleConfig = moduleConfigs.get(currentModule);
        int batchSizeModule = moduleConfig.batchSize;
        if (batchSizeModule != DEFAULT_PROP_BATCH_SIZE_NOT_SPECIFIED) {
          batchSize = batchSizeModule;
        }
      }

      if (batchSize == DEFAULT_PROP_BATCH_SIZE_INCLUDE_ALL) {
        batchSize = Integer.MAX_VALUE;
      }
      logger.info("batchSize determined to be {} for module={}", batchSize, currentModule);

      // TODO Even out the batch sizes (i.e. 20/20/1 should be replaced by 14/14/13)
      List<String> currentList = new LinkedList<>();
      for (TestInfo testInfo : entry.getValue()) {
        if (testInfo.isIsolated || testInfo.skipBatching) {
          UnitTestBatch unitTestBatch =
              new UnitTestBatch(batchIdCounter, testCasePropertyName, Collections.singletonList(testInfo.testName),
                  currentPathPrefix, !testInfo.isIsolated);
          testBatches.add(unitTestBatch);
        } else {
          currentList.add(testInfo.testName);
          if (currentList.size() == batchSize) {
            UnitTestBatch unitTestBatch =
                new UnitTestBatch(batchIdCounter, testCasePropertyName, Collections.unmodifiableList(currentList),
                    currentPathPrefix, true);
            testBatches.add(unitTestBatch);
            currentList = new LinkedList<>();
          }
        }
      }
      if (!currentList.isEmpty()) {
        UnitTestBatch unitTestBatch =
            new UnitTestBatch(batchIdCounter, testCasePropertyName, Collections.unmodifiableList(currentList),
                currentPathPrefix, true);
        testBatches.add(unitTestBatch);
      }
    }
    return testBatches;
  }


  private RootConfig getRootConfig(Context context) {
    ModuleConfig moduleConfig =
        getModuleConfig(context, "irrelevant", DEFAULT_PROP_BATCH_SIZE);

    String subDirForPrefix =
        context.getString(PROP_SUBDIR_FOR_PREFIX, DEFAULT_PROP_SUBDIR_FOR_PREFIX);
    Preconditions
        .checkArgument(StringUtils.isNotBlank(subDirForPrefix) && !subDirForPrefix.contains("/"));

    Context modulesContext =
        new Context(context.getSubProperties(Joiner.on(".").join(PROP_MODULE_LIST, "")));
    Set<String> includedModules = getProperty(modulesContext, PROP_INCLUDE);
    Set<String> excludedModules = getProperty(modulesContext, PROP_EXCLUDE);
    if (!includedModules.isEmpty() && !excludedModules.isEmpty()) {
      throw new IllegalArgumentException(String.format(
          "%s and %s are mutually exclusive for property %s. Provided values: included=%s, excluded=%s",
          PROP_INCLUDE, PROP_EXCLUDE, PROP_MODULE_LIST, includedModules, excludedModules));
    }

    return new RootConfig(includedModules, excludedModules, moduleConfig.include,
        moduleConfig.exclude, moduleConfig.skipBatching, moduleConfig.isolate,
        moduleConfig.batchSize, subDirForPrefix);
  }

  private ModuleConfig getModuleConfig(Context context, String moduleName, int defaultBatchSize) {
    Set<String> excluded = getProperty(context, PROP_EXCLUDE);
    Set<String> isolated = getProperty(context, PROP_ISOLATE);
    Set<String> included = getProperty(context, PROP_INCLUDE);
    Set<String> skipBatching = getProperty(context, PROP_SKIP_BATCHING);
    if (!included.isEmpty() && !excluded.isEmpty()) {
      throw new IllegalArgumentException(String.format("Included and excluded mutually exclusive." +
          " Included = %s, excluded = %s", included.toString(), excluded.toString()) +
          " for module: " + moduleName);
    }
    int batchSize = context.getInteger(PROP_BATCH_SIZE, defaultBatchSize);

    String pathPrefix = getPathPrefixFromModuleName(moduleName);

    return new ModuleConfig(moduleName, included, excluded, skipBatching, isolated, batchSize,
        pathPrefix);
  }

  private Set<String> getProperty(Context context, String propertyName) {
    return Sets.newHashSet(VALUE_SPLITTER.split(context.getString(propertyName, "")));
  }

  private String getPathPrefixFromModuleName(String moduleName) {
    String pathPrefix;
    if (moduleName.equals(MODULE_NAME_TOP_LEVEL)) {
      pathPrefix = PREFIX_TOP_LEVEL;
    } else {
      pathPrefix = moduleName.replace(".", "/");
    }
    return pathPrefix;
  }

  private String getModuleNameFromPathPrefix(String pathPrefix) {
    if (pathPrefix.equals(PREFIX_TOP_LEVEL)) {
      return MODULE_NAME_TOP_LEVEL;
    } else {
      pathPrefix = stripEndAndStart(pathPrefix, "/");
      pathPrefix = pathPrefix.replace("/", ".");
      // Example handling of dirs with a .
      // shims/hadoop-2.6
      //   -> moduleName=shims.hadoop-.2.6
      return pathPrefix;
    }
  }

  private String stripEndAndStart(String srcString, String stripChars) {
    srcString = StringUtils.stripEnd(srcString, stripChars);
    srcString = StringUtils.stripStart(srcString, stripChars);
    return srcString;
  }

  private Map<String, ModuleConfig> extractModuleConfigs() {
    Collection<String> modules = extractConfiguredModules();
    Map<String, ModuleConfig> result = new HashMap<>();

    for (String moduleName : modules) {
      Context moduleContext =
          new Context(unitModuleContext.getSubProperties(Joiner.on(".").join(moduleName, "")));
      ModuleConfig moduleConfig =
          getModuleConfig(moduleContext, moduleName, DEFAULT_PROP_BATCH_SIZE_NOT_SPECIFIED);
      logger.info("Adding moduleConfig={}", moduleConfig);
      result.put(moduleName, moduleConfig);
    }
    return result;
  }

  private Collection<String> extractConfiguredModules() {
    List<String> configuredModules = new LinkedList<>();

    Map<String, String> modulesMap = unitRootContext.getSubProperties(Joiner.on(".").join(
        PROP_ONE_MODULE, ""));
    for (Map.Entry<String, String> module : modulesMap.entrySet()) {
      // This is an unnecessary check, and forced configuration in the property file. Maybe
      // replace with an enforced empty value string.
      Preconditions.checkArgument(module.getKey().equals(module.getValue()));
      String moduleName = module.getKey();
      configuredModules.add(moduleName);
    }
    return configuredModules;
  }

  private List<TestDir> processPropertyDirectories() throws IOException {
    String srcDirString = sourceDirectory.getCanonicalPath();
    List<TestDir> unitTestsDirs = Lists.newArrayList();
    String propDirectoriies = unitRootContext.getString(PROP_DIRECTORIES, DEFAULT_PROP_DIRECTORIES);
    Iterable<String> propDirectoriesIterable = VALUE_SPLITTER.split(propDirectoriies);

    for (String unitTestDir : propDirectoriesIterable) {
      File unitTestParent = new File(sourceDirectory, unitTestDir);
      if (unitTestParent.isDirectory() || inTest) {
        String absUnitTestDir = unitTestParent.getCanonicalPath();

        Preconditions.checkState(absUnitTestDir.startsWith(srcDirString),
            "Unit test dir: " + absUnitTestDir + " is not under provided src dir: " + srcDirString);
        String modulePath = absUnitTestDir.substring(srcDirString.length());

        modulePath = stripEndAndStart(modulePath, "/");

        Preconditions.checkState(!modulePath.startsWith("/"),
            String.format("Illegal module path: [%s]", modulePath));
        if (StringUtils.isEmpty(modulePath)) {
          modulePath = PREFIX_TOP_LEVEL;
        }
        String moduleName = getModuleNameFromPathPrefix(modulePath);
        logger.info("modulePath determined as {} for testdir={}, DerivedModuleName={}", modulePath,
            absUnitTestDir, moduleName);


        logger.info("Adding unitTests dir [{}],[{}]", unitTestParent, moduleName);
        unitTestsDirs.add(new TestDir(unitTestParent, moduleName));
      } else {
        logger.warn("Unit test directory " + unitTestParent + " does not exist, or is a file.");
      }
    }

    return unitTestsDirs;
  }

  private void validateConfigs(RootConfig rootConfig,
                               Map<String, ModuleConfig> moduleConfigs,
                               List<TestDir> unitTestDir) {

    if (rootConfig.include.isEmpty() & rootConfig.exclude.isEmpty()) {
      // No conflicts. Module configuration is what will be used.
      // We've already verified that includes and excludes are not present at the same time for
      // individual modules.
      return;
    }

    // Validate mainly for includes / excludes working as they should.
    for (Map.Entry<String, ModuleConfig> entry : moduleConfigs.entrySet()) {
      if (rootConfig.excludedModules.contains(entry.getKey())) {
        // Don't bother validating.
        continue;
      }

      if (!rootConfig.includedModules.isEmpty() &&
          !rootConfig.includedModules.contains(entry.getKey())) {
        // Include specified, but this module is not in the set.
        continue;
      }

      // If global contains includes, individual modules can only contain additional includes.
      if (!rootConfig.include.isEmpty() && !entry.getValue().exclude.isEmpty()) {
        throw new IllegalStateException(String.format(
            "Global config specified includes, while module config for %s specified excludes",
            entry.getKey()));
      }
      // If global contains excludes, individual modules can only contain additional excludes.
      if (!rootConfig.exclude.isEmpty() && !entry.getValue().include.isEmpty()) {
        throw new IllegalStateException(String.format(
            "Global config specified excludes, while module config for %s specified includes",
            entry.getKey()));
      }
    }
  }

  private LinkedHashMap<String, LinkedHashSet<TestInfo>> generateFullTestSet(RootConfig rootConfig,
                                                                             Map<String, ModuleConfig> moduleConfigs,
                                                                             List<TestDir> unitTestDirs) throws
      IOException {
    LinkedHashMap<String, LinkedHashSet<TestInfo>> result = new LinkedHashMap<>();

    for (TestDir unitTestDir : unitTestDirs) {
      for (File classFile : fileListProvider
          .listFiles(unitTestDir.path, new String[]{"class"}, true)) {
        String className = classFile.getName();

        if (className.startsWith("Test") && !className.contains("$")) {
          String testName = className.replaceAll("\\.class$", "");
          String pathPrefix = getPathPrefix(classFile, rootConfig.subDirForPrefix);
          String moduleName = getModuleNameFromPathPrefix(pathPrefix);
          logger.debug("In {}, found class {} with pathPrefix={}, moduleName={}", unitTestDir.path,
              className,
              pathPrefix, moduleName);


          ModuleConfig moduleConfig = moduleConfigs.get(moduleName);
          if (moduleConfig == null) {
            moduleConfig = FAKE_MODULE_CONFIG;
          }
          TestInfo testInfo = checkAndGetTestInfo(moduleName, pathPrefix, testName, rootConfig, moduleConfig);
          if (testInfo != null) {
            logger.info("Adding test: " + testInfo);
            addTestToResult(result, testInfo);
          }
        } else {
          logger.trace("In {}, found class {} with pathPrefix={}. Not a test", unitTestDir.path,
              className);
        }
      }
    }
    return result;
  }

  private void addTestToResult(Map<String, LinkedHashSet<TestInfo>> result, TestInfo testInfo) {
    LinkedHashSet<TestInfo> moduleSet = result.get(testInfo.moduleName);
    if (moduleSet == null) {
      moduleSet = new LinkedHashSet<>();
      result.put(testInfo.moduleName, moduleSet);
    }
    moduleSet.add(testInfo);
  }

  private String getPathPrefix(File file, String subDirPrefix) throws IOException {
    String fname = file.getCanonicalPath();
    Preconditions.checkState(fname.startsWith(sourceDirectory.getCanonicalPath()));
    fname = fname.substring(sourceDirectory.getCanonicalPath().length(), fname.length());
    if (fname.contains(subDirPrefix)) {
      fname = fname.substring(0, fname.indexOf(subDirPrefix));
      fname = StringUtils.stripStart(fname, "/");
      if (StringUtils.isEmpty(fname)) {
        fname = PREFIX_TOP_LEVEL;
      }
      return fname;
    } else {
      logger.error("Could not find subDirPrefix {} in path: {}", subDirPrefix, fname);
      return PREFIX_TOP_LEVEL;
    }
  }

  private TestInfo checkAndGetTestInfo(String moduleName, String moduleRelDir, String testName,
                                       RootConfig rootConfig, ModuleConfig moduleConfig) {
    Preconditions.checkNotNull(moduleConfig);
    TestInfo testInfo;
    String rejectReason = null;
    try {
      if (rootConfig.excludedModules.contains(moduleName)) {
        rejectReason = "root level module exclude";
        return null;
      }
      if (!rootConfig.includedModules.isEmpty() &&
          !rootConfig.includedModules.contains(moduleName)) {
        rejectReason = "root level include, but not for module";
        return null;
      }
      if (rootConfig.exclude.contains(testName)) {
        rejectReason = "root excludes test";
        return null;
      }
      if (moduleConfig.exclude.contains(testName)) {
        rejectReason = "module excludes test";
        return null;
      }
      boolean containsInclude = !rootConfig.include.isEmpty() || !moduleConfig.include.isEmpty();
      if (containsInclude) {
        if (!(rootConfig.include.contains(testName) || moduleConfig.include.contains(testName))) {
          rejectReason = "test missing from include list";
          return null;
        }
      }
      if (excludedProvided.contains(testName)) {
        // All qfiles handled via this...
        rejectReason = "test present in provided exclude list";
        return null;
      }

      // Add the test.
      testInfo = new TestInfo(moduleName, moduleRelDir, testName, rootConfig.skipBatching.contains(testName) ||
          moduleConfig.skipBatching.contains(testName),
          rootConfig.isolate.contains(testName) || moduleConfig.isolate.contains(testName));
      return testInfo;

    } finally {
      if (rejectReason != null) {
        logger.debug("excluding {} due to {}", testName, rejectReason);
      }
    }
  }

  private static final class RootConfig {
    private final Set<String> includedModules;
    private final Set<String> excludedModules;
    private final Set<String> include;
    private final Set<String> exclude;
    private final Set<String> skipBatching;
    private final Set<String> isolate;
    private final int batchSize;
    private final String subDirForPrefix;

    RootConfig(Set<String> includedModules, Set<String> excludedModules, Set<String> include,
               Set<String> exclude, Set<String> skipBatching, Set<String> isolate,
               int batchSize, String subDirForPrefix) {
      this.includedModules = includedModules;
      this.excludedModules = excludedModules;
      this.include = include;
      this.exclude = exclude;
      this.skipBatching = skipBatching;
      this.isolate = isolate;
      this.batchSize = batchSize;
      this.subDirForPrefix = subDirForPrefix;
    }

    @Override
    public String toString() {
      return "RootConfig{" +
          "includedModules=" + includedModules +
          ", excludedModules=" + excludedModules +
          ", include=" + include +
          ", exclude=" + exclude +
          ", skipBatching=" + skipBatching +
          ", isolate=" + isolate +
          ", batchSize=" + batchSize +
          ", subDirForPrefix='" + subDirForPrefix + '\'' +
          '}';
    }
  }

  private static final ModuleConfig FAKE_MODULE_CONFIG =
      new ModuleConfig("_FAKE_", new HashSet<String>(), new HashSet<String>(),
          new HashSet<String>(), new HashSet<String>(), DEFAULT_PROP_BATCH_SIZE_NOT_SPECIFIED,
          "_fake_");

  private static final class ModuleConfig {
    private final String name;
    private final Set<String> include;
    private final Set<String> exclude;
    private final Set<String> skipBatching;
    private final Set<String> isolate;
    private final String pathPrefix;
    private final int batchSize;

    ModuleConfig(String name, Set<String> include, Set<String> exclude,
                 Set<String> skipBatching, Set<String> isolate, int batchSize,
                 String pathPrefix) {
      this.name = name;
      this.include = include;
      this.exclude = exclude;
      this.skipBatching = skipBatching;
      this.isolate = isolate;
      this.batchSize = batchSize;
      this.pathPrefix = pathPrefix;
    }

    @Override
    public String toString() {
      return "ModuleConfig{" +
          "name='" + name + '\'' +
          ", include=" + include +
          ", exclude=" + exclude +
          ", skipBatching=" + skipBatching +
          ", isolate=" + isolate +
          ", pathPrefix='" + pathPrefix + '\'' +
          ", batchSize=" + batchSize +
          '}';
    }
  }

  private static class TestDir {
    final File path;
    final String module;

    TestDir(File path, String module) {
      this.path = path;
      this.module = module;
    }

    @Override
    public String toString() {
      return "TestDir{" +
          "path=" + path +
          ", module='" + module + '\'' +
          '}';
    }
  }

  private static class TestInfo {
    final String moduleName;
    final String moduleRelativeDir;
    final String testName;
    final boolean skipBatching;
    final boolean isIsolated;

    TestInfo(String moduleName, String moduleRelativeDir, String testName, boolean skipBatching, boolean isIsolated) {
      this.moduleName = moduleName;
      this.moduleRelativeDir = moduleRelativeDir;
      this.testName = testName;
      this.skipBatching = skipBatching;
      this.isIsolated = isIsolated;
    }

    @Override
    public String toString() {
      return "TestInfo{" +
          "moduleName='" + moduleName + '\'' +
          ", moduleRelativeDir='" + moduleRelativeDir + '\'' +
          ", testName='" + testName + '\'' +
          ", skipBatching=" + skipBatching +
          ", isIsolated=" + isIsolated +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TestInfo testInfo = (TestInfo) o;

      return skipBatching == testInfo.skipBatching && isIsolated == testInfo.isIsolated &&
          moduleName.equals(testInfo.moduleName) &&
          moduleRelativeDir.equals(testInfo.moduleRelativeDir) &&
          testName.equals(testInfo.testName);

    }

    @Override
    public int hashCode() {
      int result = moduleName.hashCode();
      result = 31 * result + moduleRelativeDir.hashCode();
      result = 31 * result + testName.hashCode();
      result = 31 * result + (skipBatching ? 1 : 0);
      result = 31 * result + (isIsolated ? 1 : 0);
      return result;
    }
  }

  private static final class DefaultFileListProvider implements FileListProvider {

    @Override
    public Collection<File> listFiles(File directory, String[] extensions, boolean recursive) {
      return FileUtils.listFiles(directory, extensions, recursive);
    }
  }
}
