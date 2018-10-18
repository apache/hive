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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestUnitTestPropertiesParser {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestUnitTestPropertiesParser.class);

  private static final String MODULE1_NAME = "module1";
  private static final String MODULE1_TEST_NAME = "Module1";
  private static final String MODULE2_NAME = "module2";
  private static final String MODULE2_TEST_NAME = "Module2";

  private static final String TOP_LEVEL_TEST_NAME = "tl";
  private static final String TWO_LEVEL_MODULE1_NAME = "module2l.submodule1";
  private static final String TWO_LEVEL_TEST_NAME = "TwoLevel";
  private static final String THREE_LEVEL_MODULE1_NAME = "module3l.sub.submodule1";
  private static final String THREE_LEVEL_TEST_NAME = "ThreeLevel";

  private static final String MODULE3_REL_DIR = "TwoLevel/module-2.6";
  private static final String MODULE3_MODULE_NAME = "TwoLevel.module-2.6";
  private static final String MODULE3_TEST_NAME = "Module3";


  private static final int BATCH_SIZE_DEFAULT = 10;

  private static final String TEST_CASE_PROPERT_NAME = "test";

  @Test(timeout = 5000)
  public void testSimpleSetup() {

    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);


    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        2,
        new String[]{MODULE1_NAME, MODULE2_NAME},
        new int[]{5, 4},
        new boolean[]{true, true});
  }

  @Test(timeout = 5000)
  public void testTopLevelExclude() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_EXCLUDE),
        "Test" + MODULE1_TEST_NAME + "1");

    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        2,
        new String[]{MODULE1_NAME, MODULE2_NAME},
        new int[]{4, 4},
        new boolean[]{true, true});
  }

  @Test(timeout = 5000)
  public void testTopLevelInclude() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_INCLUDE),
        "Test" + MODULE1_TEST_NAME + "1" + " " + "Test" + MODULE1_TEST_NAME + "2");

    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        1,
        new String[]{MODULE1_NAME},
        new int[]{2},
        new boolean[]{true});
  }

  @Test(timeout = 5000)
  public void testTopLevelSkipBatching() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_SKIP_BATCHING),
        "Test" + MODULE1_TEST_NAME + "1" + " " + "Test" + MODULE1_TEST_NAME + "2");

    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        4,
        new String[]{MODULE1_NAME, MODULE1_NAME, MODULE1_NAME, MODULE2_NAME},
        new int[]{1, 1, 3, 4},
        new boolean[]{true, true, true, true});
  }

  @Test(timeout = 5000)
  public void testTopLevelIsolate() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_ISOLATE),
        "Test" + MODULE1_TEST_NAME + "1" + " " + "Test" + MODULE1_TEST_NAME + "2");

    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        4,
        new String[]{MODULE1_NAME, MODULE1_NAME, MODULE1_NAME, MODULE2_NAME},
        new int[]{1, 1, 3, 4},
        new boolean[]{false, false, true, true});
  }

  @Test(timeout = 5000)
  public void testTopLevelBatchSize() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();
    context
        .put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_BATCH_SIZE), Integer.toString(2));


    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        5,
        new String[]{MODULE1_NAME, MODULE1_NAME, MODULE1_NAME, MODULE2_NAME, MODULE2_NAME},
        new int[]{2, 2, 1, 2, 2},
        new boolean[]{true, true, true, true, true});
  }

  @Test(timeout = 5000)
  public void testModuleLevelExclude() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_ONE_MODULE, MODULE1_NAME),
        MODULE1_NAME);
    context.put(getUtSpecificPropertyName(MODULE1_NAME, UnitTestPropertiesParser.PROP_EXCLUDE),
        "Test" + MODULE1_TEST_NAME + "1");

    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        2,
        new String[]{MODULE1_NAME, MODULE2_NAME},
        new int[]{4, 4},
        new boolean[]{true, true});
  }

  @Test(timeout = 5000)
  public void testModuleLevelInclude() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_ONE_MODULE, MODULE1_NAME),
        MODULE1_NAME);
    context.put(getUtSpecificPropertyName(MODULE1_NAME, UnitTestPropertiesParser.PROP_INCLUDE),
        "Test" + MODULE1_TEST_NAME + "1" + " " + "Test" + MODULE1_TEST_NAME + "2");

    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        2,
        new String[]{MODULE1_NAME, MODULE2_NAME},
        new int[]{2, 4},
        new boolean[]{true, true});
  }

  @Test(timeout = 5000)
  public void testModuleLevelSkipBatching() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_ONE_MODULE, MODULE1_NAME),
        MODULE1_NAME);
    context
        .put(getUtSpecificPropertyName(MODULE1_NAME, UnitTestPropertiesParser.PROP_SKIP_BATCHING),
            "Test" + MODULE1_TEST_NAME + "1" + " " + "Test" + MODULE1_TEST_NAME + "2");

    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        4,
        new String[]{MODULE1_NAME, MODULE1_NAME, MODULE1_NAME, MODULE2_NAME},
        new int[]{1, 1, 3, 4},
        new boolean[]{true, true, true, true});
  }

  @Test(timeout = 5000)
  public void testModuleLevelIsolate() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_ONE_MODULE, MODULE1_NAME),
        MODULE1_NAME);
    context.put(getUtSpecificPropertyName(MODULE1_NAME, UnitTestPropertiesParser.PROP_ISOLATE),
        "Test" + MODULE1_TEST_NAME + "1" + " " + "Test" + MODULE1_TEST_NAME + "2");

    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        4,
        new String[]{MODULE1_NAME, MODULE1_NAME, MODULE1_NAME, MODULE2_NAME},
        new int[]{1, 1, 3, 4},
        new boolean[]{false, false, true, true});
  }

  @Test(timeout = 5000)
  public void testModuleLevelBatchSize() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_ONE_MODULE, MODULE1_NAME),
        MODULE1_NAME);
    context.put(getUtSpecificPropertyName(MODULE1_NAME, UnitTestPropertiesParser.PROP_BATCH_SIZE),
        Integer.toString(2));


    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        4,
        new String[]{MODULE1_NAME, MODULE1_NAME, MODULE1_NAME, MODULE2_NAME},
        new int[]{2, 2, 1, 4},
        new boolean[]{true, true, true, true});
  }

  @Test(timeout = 5000)
  public void testProvidedExclude() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider = getTestFileListProvider(baseDir, 5, 4);

    Set<String> excludedProvided = Sets.newHashSet("Test" + MODULE1_TEST_NAME + "1");
    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            excludedProvided, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        2,
        new String[]{MODULE1_NAME, MODULE2_NAME},
        new int[]{4, 4},
        new boolean[]{true, true});
  }

  @Test(timeout = 5000)
  public void testTopLevelBatchSizeIncludeAll() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider = getTestFileListProvider(baseDir, 120, 60);
    context
        .put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_BATCH_SIZE), Integer.toString(0));

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        2,
        new String[]{MODULE1_NAME, MODULE2_NAME},
        new int[]{120, 60},
        new boolean[]{true, true});
  }

  @Test(timeout = 5000)
  public void testModuleLevelBatchSizeIncludeAll() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider = getTestFileListProvider(baseDir, 50, 4);
    context
        .put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_BATCH_SIZE), Integer.toString(2));
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_ONE_MODULE, MODULE1_NAME),
        MODULE1_NAME);
    context.put(getUtSpecificPropertyName(MODULE1_NAME, UnitTestPropertiesParser.PROP_BATCH_SIZE),
        Integer.toString(0));

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);

    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        3,
        new String[]{MODULE1_NAME, MODULE2_NAME, MODULE2_NAME},
        new int[]{50, 2, 2},
        new boolean[]{true, true, true});
  }

  @Test(timeout = 5000)
  public void testMultiLevelModules() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider = getTestFileListProviderMultiLevel(baseDir, 4, 30, 6, 9);
    context
        .put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_BATCH_SIZE), Integer.toString(4));
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_ONE_MODULE, MODULE1_NAME),
        MODULE1_NAME);
    context.put(getUtSpecificPropertyName(MODULE1_NAME, UnitTestPropertiesParser.PROP_BATCH_SIZE),
        Integer.toString(0));

    context.put(
        getUtRootPropertyName(UnitTestPropertiesParser.PROP_ONE_MODULE, THREE_LEVEL_MODULE1_NAME),
        THREE_LEVEL_MODULE1_NAME);
    context.put(getUtSpecificPropertyName(THREE_LEVEL_MODULE1_NAME,
        UnitTestPropertiesParser.PROP_BATCH_SIZE),
        Integer.toString(0));

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);
    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        5,
        new String[]{UnitTestPropertiesParser.PREFIX_TOP_LEVEL, MODULE1_NAME,
            TWO_LEVEL_MODULE1_NAME, TWO_LEVEL_MODULE1_NAME, THREE_LEVEL_MODULE1_NAME},
        new int[]{4, 30, 4, 2, 9},
        new boolean[]{true, true, true, true, true});

  }

  @Test(timeout = 5000)
  public void testTopLevelModuleConfig() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider = getTestFileListProviderMultiLevel(baseDir, 9, 0, 0, 0);
    context
        .put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_BATCH_SIZE), Integer.toString(4));
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_ONE_MODULE,
        UnitTestPropertiesParser.MODULE_NAME_TOP_LEVEL),
        UnitTestPropertiesParser.MODULE_NAME_TOP_LEVEL);
    context.put(getUtSpecificPropertyName(UnitTestPropertiesParser.MODULE_NAME_TOP_LEVEL,
        UnitTestPropertiesParser.PROP_BATCH_SIZE),
        Integer.toString(0));

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);
    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        1,
        new String[]{UnitTestPropertiesParser.PREFIX_TOP_LEVEL},
        new int[]{9},
        new boolean[]{true});
  }

  @Test(timeout = 5000)
  public void testScanMultipleDirectoriesNested() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider = getTestFileListProviderMultiLevel(baseDir, 13, 5, 0, 0);
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_DIRECTORIES),
        "./ ./" + MODULE1_NAME);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);
    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        3,
        new String[]{UnitTestPropertiesParser.PREFIX_TOP_LEVEL,
            UnitTestPropertiesParser.PREFIX_TOP_LEVEL, MODULE1_NAME},
        new int[]{10, 3, 5},
        new boolean[]{true, true, true});
  }

  @Test(timeout = 5000)
  public void testScanMultipleDirectoriesNonNested() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider = getTestFileListProvider(baseDir, 13, 8);
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_DIRECTORIES),
        "./" + MODULE1_NAME + " " + "./" + MODULE2_NAME);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);
    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        3,
        new String[]{MODULE1_NAME, MODULE1_NAME, MODULE2_NAME},
        new int[]{10, 3, 8},
        new boolean[]{true, true, true});
  }

  @Test(timeout = 5000)
  public void testModuleInclude() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider = getTestFileListProvider(baseDir, 13, 8);
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_MODULE_LIST,
        UnitTestPropertiesParser.PROP_INCLUDE), MODULE1_NAME);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);
    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        2,
        new String[]{MODULE1_NAME, MODULE1_NAME},
        new int[]{10, 3},
        new boolean[]{true, true});
  }

  @Test(timeout = 5000)
  public void testModuleExclude() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider = getTestFileListProvider(baseDir, 13, 8);
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_MODULE_LIST,
        UnitTestPropertiesParser.PROP_EXCLUDE), MODULE1_NAME);

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);
    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        1,
        new String[]{MODULE2_NAME},
        new int[]{8},
        new boolean[]{true});
  }

  @Test(timeout = 5000)
  public void testModuleWithPeriodInDirName() {
    File baseDir = getFakeTestBaseDir();
    Context context = getDefaultContext();

    FileListProvider flProvider =
        getTestFileListProviderSingleModule(baseDir, MODULE3_REL_DIR, MODULE3_TEST_NAME, 13);
    context
        .put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_ONE_MODULE, MODULE3_MODULE_NAME),
            MODULE3_MODULE_NAME);
    context.put(
        getUtSpecificPropertyName(MODULE3_MODULE_NAME, UnitTestPropertiesParser.PROP_BATCH_SIZE),
        Integer.toString(5));

    UnitTestPropertiesParser parser =
        new UnitTestPropertiesParser(context, new AtomicInteger(1), TEST_CASE_PROPERT_NAME, baseDir, LOG, flProvider,
            null, true);
    Collection<TestBatch> testBatchCollection = parser.generateTestBatches();
    verifyBatches(testBatchCollection,
        3,
        new String[]{MODULE3_MODULE_NAME, MODULE3_MODULE_NAME, MODULE3_MODULE_NAME},
        new int[]{5, 5, 3},
        new boolean[]{true, true, true});

  }

  private void verifyBatches(Collection<TestBatch> testBatchCollection, int numBatches,
                             String[] moduleNames, int[] testsPerBatch, boolean[] isParallel) {
    List<TestBatch> testBatches = new LinkedList<>(testBatchCollection);
    assertEquals(String.format("Expected batches=[%d], found=[%d]", numBatches, testBatches.size()),
        numBatches, testBatches.size());
    assert moduleNames.length == numBatches;
    assert testsPerBatch.length == numBatches;
    assert isParallel.length == numBatches;

    for (int i = 0; i < numBatches; i++) {
      TestBatch testBatch = testBatches.get(i);
      if (!moduleNames[i].equals(UnitTestPropertiesParser.PREFIX_TOP_LEVEL)) {
        moduleNames[i] = moduleNames[i].replace(".", "/");
      }

      assertEquals(String.format("Expected batchName=[%s], found=[%s] on index=%d", moduleNames[i],
          testBatch.getTestModuleRelativeDir(), i), moduleNames[i],
          testBatch.getTestModuleRelativeDir());
      assertEquals(String.format("Expected size=[%d], found=[%d] on index=%d", testsPerBatch[i],
          testBatch.getNumTestsInBatch(), i), testsPerBatch[i], testBatch.getNumTestsInBatch());
      assertEquals(String.format("Expected isParallel=[%s], found=[%s] on index=%d", isParallel[i],
          testBatch.isParallel(), i), isParallel[i], testBatch.isParallel());
    }
  }


  private static File getFakeTestBaseDir() {
    File javaTmpDir = new File(System.getProperty("java.io.tmpdir"));
    File baseDir = new File(javaTmpDir, UUID.randomUUID().toString());
    return baseDir;
  }

  /**
   * Returns 2 modules. Counts can be specified.
   *
   * @param module1Count
   * @param module2Count
   * @return
   */
  private static FileListProvider getTestFileListProvider(final File baseDir,
                                                          final int module1Count,
                                                          final int module2Count) {

    return new FileListProvider() {
      @Override
      public Collection<File> listFiles(File directory, String[] extensions, boolean recursive) {
        List<File> list = new LinkedList<>();

        File m1F = new File(baseDir, Joiner.on("/").join(MODULE1_NAME, "target", "test", "p1"));
        for (int i = 0; i < module1Count; i++) {
          list.add(new File(m1F, "Test" + MODULE1_TEST_NAME + (i + 1) + ".class"));
        }

        File m2F = new File(baseDir, Joiner.on("/").join(MODULE2_NAME, "target", "test"));
        for (int i = 0; i < module2Count; i++) {
          list.add(new File(m2F, "Test" + MODULE2_TEST_NAME + (i + 1) + ".class"));
        }

        return list;
      }
    };
  }

  private static FileListProvider getTestFileListProviderMultiLevel(final File baseDir,
                                                                    final int l0Count,
                                                                    final int l1Count,
                                                                    final int l2Count,
                                                                    final int l3Count) {
    return new FileListProvider() {
      @Override
      public Collection<File> listFiles(File directory, String[] extensions, boolean recursive) {
        List<File> list = new LinkedList<>();

        File l0F = new File(baseDir, Joiner.on("/").join("target", "test", "p1", "p2"));
        for (int i = 0; i < l0Count; i++) {
          list.add(new File(l0F, "Test" + TOP_LEVEL_TEST_NAME + (i + 1) + ".class"));
        }


        File l1F = new File(baseDir, Joiner.on("/").join(MODULE1_NAME, "target", "test"));
        for (int i = 0; i < l1Count; i++) {
          list.add(new File(l1F, "Test" + MODULE1_TEST_NAME + (i + 1) + ".class"));
        }

        File l2F = new File(baseDir, Joiner.on("/").join(TWO_LEVEL_MODULE1_NAME, "target", "test"));
        for (int i = 0; i < l2Count; i++) {
          list.add(new File(l2F, "Test" + TWO_LEVEL_TEST_NAME + (i + 1) + ".class"));
        }

        File l3F =
            new File(baseDir, Joiner.on("/").join(THREE_LEVEL_MODULE1_NAME, "target", "test"));
        for (int i = 0; i < l3Count; i++) {
          list.add(new File(l3F, "Test" + THREE_LEVEL_TEST_NAME + (i + 1) + ".class"));
        }
        return list;
      }
    };
  }

  private static FileListProvider getTestFileListProviderSingleModule(final File baseDir,
                                                                      final String moduleRelDir,
                                                                      final String testName,
                                                                      final int numTests) {
    return new FileListProvider() {

      @Override
      public Collection<File> listFiles(File directory, String[] extensions, boolean recursive) {
        List<File> list = new LinkedList<>();
        File f = new File(baseDir, Joiner.on("/").join(moduleRelDir, "target", "package", "test"));
        for (int i = 0; i < numTests; i++) {
          list.add(new File(f, "Test" + testName + (i + 1) + ".class"));
        }
        return list;
      }
    };
  }

  private static Context getDefaultContext() {
    Context context = new Context();
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_DIRECTORIES), "./");
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_SUBDIR_FOR_PREFIX), "target");
    context.put(getUtRootPropertyName(UnitTestPropertiesParser.PROP_BATCH_SIZE),
        Integer.toString(BATCH_SIZE_DEFAULT));
    return context;
  }

  private static String getUtRootPropertyName(String p1, String... rest) {
    return Joiner.on(".").join(UnitTestPropertiesParser.PROP_PREFIX_ROOT, p1, rest);
  }

  private static String getUtSpecificPropertyName(String p1, String... rest) {
    return Joiner.on(".").join(UnitTestPropertiesParser.PROP_PREFIX_MODULE, p1, rest);
  }
}
