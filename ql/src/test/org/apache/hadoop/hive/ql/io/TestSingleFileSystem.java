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

package org.apache.hadoop.hive.ql.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestSingleFileSystem {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private File f1;
  private String f1path;
  private FileSystem fs;

  @Test
  public void testAllRegistered() {
    ServiceLoader<FileSystem> fs = ServiceLoader.load(FileSystem.class);
    Set<Class<?>> knownFileSystems = new HashSet<>();

    for (FileSystem fileSystem : fs) {
      knownFileSystems.add(fileSystem.getClass());
    }

    for (Class<?> sfsClass : SingleFileSystem.class.getDeclaredClasses()) {
      if (SingleFileSystem.class.isAssignableFrom(sfsClass)) {
        if (!knownFileSystems.contains(sfsClass)) {
          fail(sfsClass + " is not registered!");
        }
      }
    }
  }

  @Before
  public void before() throws Exception {
    f1 = folder.newFile("f1");
    Files.write("asd", f1, Charsets.ISO_8859_1);
    f1path = f1.toURI().toString();
    Path p = new Path("sfs+" + f1path);
    fs = p.getFileSystem(new Configuration());
  }

  @Test
  public void testGetFileStatus() throws Exception {
    assertSfsDir(fs.getFileStatus(new Path("sfs+" + folder.getRoot().toURI())));
    assertSfsDir(fs.getFileStatus(new Path("sfs+" + f1path)));
    assertSfsDir(fs.getFileStatus(new Path("sfs+" + f1path + "/#SINGLEFILE#")));
    assertSfsFile(fs.getFileStatus(new Path("sfs+" + f1path + "/#SINGLEFILE#/f1")));
  }

  @Test(expected = FileNotFoundException.class)
  public void testGetFileStatusOfUnknown() throws Exception {
    assertSfsFile(fs.getFileStatus(new Path("sfs+" + f1path + "/#SINGLEFILE#/unknown")));
  }

  @Test
  public void testListStatusSingleFileDir() throws Exception {
    String targetSfsPath = "sfs+" + f1path + "/#SINGLEFILE#";
    FileStatus[] list = fs.listStatus(new Path(targetSfsPath));
    assertEquals(1, list.length);
    assertEquals(targetSfsPath + "/f1", list[0].getPath().toString());
  }


  @Test
  public void testListStatusSingleFileDirEndingInSlash() throws Exception {
    String targetSfsPath = "sfs+" + f1path + "/#SINGLEFILE#/";
    FileStatus[] list = fs.listStatus(new Path(targetSfsPath));
    assertEquals(1, list.length);
    assertEquals(targetSfsPath + "f1", list[0].getPath().toString());
  }

  @Test(expected = FileNotFoundException.class)
  public void testListSingleFileDirOfNonExistentFile() throws Exception {
    String targetSfsPath = "sfs+" + f1path + "nonExistent/#SINGLEFILE#/";
    fs.listStatus(new Path(targetSfsPath));
  }

  @Test
  public void testListStatusTargetFile() throws Exception {
    String targetSfsPath = "sfs+" + f1path + "/#SINGLEFILE#/f1";
    FileStatus[] list = fs.listStatus(new Path(targetSfsPath));
    assertEquals(1, list.length);
    assertEquals(targetSfsPath, list[0].getPath().toString());
  }

  @Test(expected = FileNotFoundException.class)
  public void testListStatusNonTargetFile() throws Exception {
    String targetSfsPath = "sfs+" + f1path + "/#SINGLEFILE#/unknown";
    fs.listStatus(new Path(targetSfsPath));
  }

  @Test
  public void testListStatusFile() throws Exception {
    String targetSfsPath = "sfs+" + f1path;
    FileStatus[] list = fs.listStatus(new Path(targetSfsPath));
    assertEquals(1, list.length);
    assertEquals(targetSfsPath + "/#SINGLEFILE#", list[0].getPath().toString());
  }

  @Test
  public void testListStatusRoot() throws Exception {
    folder.newFolder("folder");
    String targetSfsPath = "sfs+" + folder.getRoot().toURI().toString();
    FileStatus[] list = fs.listStatus(new Path(targetSfsPath));
    assertEquals(2, list.length);

    Set<String> expectedPaths = new HashSet<String>();
    expectedPaths.add(targetSfsPath + "f1");
    expectedPaths.add(targetSfsPath + "folder");
    Set<String> paths = new HashSet<String>();
    for (FileStatus fileStatus : list) {
      paths.add(fileStatus.getPath().toString());
    }
    assertEquals(expectedPaths, paths);
  }

  @Test
  public void testOpenTargetFile() throws Exception {
    try (FSDataInputStream ret = fs.open(new Path("sfs+" + f1path + "/#SINGLEFILE#/f1"))) {
      try (Scanner sc = new Scanner(ret)) {
        String line = sc.nextLine();
        assertEquals("asd", line);
      }
    }
  }

  @Test(expected = FileNotFoundException.class)
  public void testOpenUnknownFileInSingleFileDir() throws Exception {
    fs.open(new Path("sfs+" + f1path + "/#SINGLEFILE#/unknown"));
  }

  @Test(expected = IOException.class)
  public void testOpenSinglefileDir() throws Exception {
    fs.open(new Path("sfs+" + f1path + "/#SINGLEFILE#/"));
  }

  @Test(expected = IOException.class)
  public void testOpenRealTargetFile() throws Exception {
    fs.open(new Path("sfs+" + f1path));
  }

  private void assertSfsDir(FileStatus fileStatus) {
    assertTrue(fileStatus.isDirectory());
    assertTrue(fileStatus.getPath().toUri().getScheme().startsWith("sfs+"));
  }

  private void assertSfsFile(FileStatus fileStatus) {
    assertTrue(!fileStatus.isDirectory());
    assertTrue(fileStatus.getPath().toUri().getScheme().startsWith("sfs+"));
  }

}
