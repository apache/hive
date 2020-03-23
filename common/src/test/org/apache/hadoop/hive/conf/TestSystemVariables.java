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
package org.apache.hadoop.hive.conf;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.fail;

public class TestSystemVariables {
  public static final String SYSTEM = "system";

  private String makeVarName(String prefix, String value) {
    return String.format("${%s:%s}", prefix, value);
  }

  @Test
  public void test_RelativeJavaIoTmpDir_CoercedTo_AbsolutePath() {
    FileSystem localFileSystem = new LocalFileSystem();
    String systemJavaIoTmpDir = makeVarName(SYSTEM, "java.io.tmpdir");

    System.setProperty("java.io.tmpdir", "./relativePath");
    Path relativePath = new Path(localFileSystem.getWorkingDirectory(), "./relativePath");
    assertEquals(relativePath.toString(), SystemVariables.substitute(systemJavaIoTmpDir));

    System.setProperty("java.io.tmpdir", "this/is/a/relative/path");
    Path thisIsARelativePath= new Path(localFileSystem.getWorkingDirectory(), "this/is/a/relative/path");
    assertEquals(thisIsARelativePath.toString(), SystemVariables.substitute(systemJavaIoTmpDir));
  }

  @Test
  public void test_AbsoluteJavaIoTmpDir_NotChanged() {
    FileSystem localFileSystem = new LocalFileSystem();
    String systemJavaIoTmpDir = makeVarName(SYSTEM, "java.io.tmpdir");

    System.setProperty("java.io.tmpdir", "file:/this/is/an/absolute/path");
    Path absolutePath = new Path("file:/this/is/an/absolute/path");
    assertEquals(absolutePath.toString(), SystemVariables.substitute(systemJavaIoTmpDir));
  }

  @Test
  public void test_RelativePathWithNoCoercion_NotChanged() {
    FileSystem localFileSystem = new LocalFileSystem();
    String systemJavaIoTmpDir = makeVarName(SYSTEM, "java.io._NOT_tmpdir");

    System.setProperty("java.io._NOT_tmpdir", "this/is/an/relative/path");
    Path relativePath = new Path("this/is/an/relative/path");
    assertEquals(relativePath.toString(), SystemVariables.substitute(systemJavaIoTmpDir));
  }

  @Test
  public void test_EmptyJavaIoTmpDir_NotChanged() {
    FileSystem localFileSystem = new LocalFileSystem();
    String systemJavaIoTmpDir = makeVarName(SYSTEM, "java.io.tmpdir");

    System.setProperty("java.io.tmpdir", "");
    assertEquals("", SystemVariables.substitute(systemJavaIoTmpDir));
  }

  @Test
  public void test_SubstituteLongSelfReference() {
    String randomPart = RandomStringUtils.random(100_000);
    String reference = "${hiveconf:myTestVariable}";

    StringBuilder longStringWithReferences = new StringBuilder();
    for(int i = 0; i < 10; i ++) {
      longStringWithReferences.append(randomPart).append(reference);
    }

    SystemVariables uut = new SystemVariables();

    HiveConf conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_QUERY_MAX_LENGTH.varname, "100Kb");
    conf.set("myTestVariable", longStringWithReferences.toString());

    try {
      uut.substitute(conf, longStringWithReferences.toString(), 40);
    } catch (Exception e) {
      if (!e.getMessage().startsWith("Query length longer than hive.query.max.length")) {
        fail("unexpected error message: " + e.getMessage());
      }
      return;
    }
    fail("should have thrown exception during substitution");
  }
}
