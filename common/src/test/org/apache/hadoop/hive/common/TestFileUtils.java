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

package org.apache.hadoop.hive.common;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

import junit.framework.TestCase;

public class TestFileUtils extends TestCase {
  public static final Logger LOG = LoggerFactory.getLogger(TestFileUtils.class);

  @Test
  public void testGetJarFilesByPath() {
    HiveConf conf = new HiveConf(this.getClass());
    File tmpDir = Files.createTempDir();
    String jarFileName1 = tmpDir.getAbsolutePath() + File.separator + "a.jar";
    String jarFileName2 = tmpDir.getAbsolutePath() + File.separator + "b.jar";
    File jarFile1 = new File(jarFileName1);
    try {
      org.apache.commons.io.FileUtils.touch(jarFile1);
      Set<String> jars = FileUtils.getJarFilesByPath(tmpDir.getAbsolutePath(), conf);
      Assert.assertEquals(Sets.newHashSet("file://" + jarFileName1), jars);

      jars = FileUtils.getJarFilesByPath("/folder/not/exist", conf);
      Assert.assertTrue(jars.isEmpty());

      File jarFile2 = new File(jarFileName2);
      org.apache.commons.io.FileUtils.touch(jarFile2);
      String newPath = "file://" + jarFileName1 + "," + "file://" + jarFileName2 + ",/file/not/exist";
      jars = FileUtils.getJarFilesByPath(newPath, conf);
      Assert.assertEquals(Sets.newHashSet("file://" + jarFileName1, "file://" + jarFileName2), jars);
    } catch (IOException e) {
      LOG.error("failed to copy file to reloading folder", e);
      Assert.fail(e.getMessage());
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(tmpDir);
    }
  }

  @Test
  public void testRelativePathToAbsolutePath() throws IOException {
    LocalFileSystem localFileSystem = new LocalFileSystem();
    Path actualPath = FileUtils.makeAbsolute(localFileSystem, new Path("relative/path"));
    Path expectedPath = new Path(localFileSystem.getWorkingDirectory(), "relative/path");
    assertEquals(expectedPath.toString(), actualPath.toString());

    Path absolutePath = new Path("/absolute/path");
    Path unchangedPath = FileUtils.makeAbsolute(localFileSystem, new Path("/absolute/path"));

    assertEquals(unchangedPath.toString(), absolutePath.toString());
  }
}
