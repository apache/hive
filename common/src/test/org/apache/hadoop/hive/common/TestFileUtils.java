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

package org.apache.hadoop.hive.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.HadoopShims;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.MockFileSystem;
import org.apache.hive.common.util.MockFileSystem.MockFile;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestFileUtils {

  public static final Logger LOG = LoggerFactory.getLogger(TestFileUtils.class);

  @Test
  public void isPathWithinSubtree_samePrefix() {
    Path path = new Path("/somedir1");
    Path subtree = new Path("/somedir");

    assertFalse(FileUtils.isPathWithinSubtree(path, subtree));
  }

  @Test
  public void isPathWithinSubtree_rootIsInside() {
    Path path = new Path("/foo");
    Path subtree = new Path("/foo");
    assertTrue(FileUtils.isPathWithinSubtree(path, subtree));
  }

  @Test
  public void isPathWithinSubtree_descendantInside() {
    Path path = new Path("/foo/bar");
    Path subtree = new Path("/foo");
    assertTrue(FileUtils.isPathWithinSubtree(path, subtree));
  }

  @Test
  public void isPathWithinSubtree_relativeWalk() {
    Path path = new Path("foo/../../bar");
    Path subtree = new Path("../bar");
    assertTrue(FileUtils.isPathWithinSubtree(path, subtree));
  }

  @Test
  public void getParentRegardlessOfScheme_badCases() {
    Path path = new Path("proto://host1/foo/bar/baz");
    ArrayList<Path> candidates = new ArrayList<>();
    candidates.add(new Path("badproto://host1/foo"));
    candidates.add(new Path("proto://badhost1/foo"));
    candidates.add(new Path("proto://host1:71/foo/bar/baz"));
    candidates.add(new Path("proto://host1/badfoo"));
    candidates.add(new Path("/badfoo"));
    Path res = FileUtils.getParentRegardlessOfScheme(path, candidates);
    assertNull("none of these paths may match", res);
  }

  @Test
  public void getParentRegardlessOfScheme_priority() {
    Path path = new Path("proto://host1/foo/bar/baz");
    ArrayList<Path> candidates = new ArrayList<>();
    Path expectedPath;
    candidates.add(new Path("proto://host1/"));
    candidates.add(expectedPath = new Path("proto://host1/foo"));
    Path res = FileUtils.getParentRegardlessOfScheme(path, candidates);
    assertEquals(expectedPath, res);
  }

  @Test
  public void getParentRegardlessOfScheme_root() {
    Path path = new Path("proto://host1/foo");
    ArrayList<Path> candidates = new ArrayList<>();
    Path expectedPath;
    candidates.add(expectedPath = new Path("proto://host1/foo"));
    Path res = FileUtils.getParentRegardlessOfScheme(path, candidates);
    assertEquals(expectedPath, res);
  }

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

  @Test
  public void testIsPathWithinSubtree() throws IOException {
    Path splitPath = new Path("file:///user/hive/warehouse/src/data.txt");
    Path splitPathWithNoSchema = Path.getPathWithoutSchemeAndAuthority(splitPath);

    Set<Path> parents = new HashSet<>();
    FileUtils.populateParentPaths(parents, splitPath);
    FileUtils.populateParentPaths(parents, splitPathWithNoSchema);

    Path key = new Path("/user/hive/warehouse/src");
    verifyIsPathWithInSubTree(splitPath, key, false);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, true);
    verifyIfParentsContainPath(key, parents, true);

    key = new Path("/user/hive/warehouse/src_2");
    verifyIsPathWithInSubTree(splitPath, key, false);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, false);
    verifyIfParentsContainPath(key, parents, false);

    key = new Path("/user/hive/warehouse/src/data.txt");
    verifyIsPathWithInSubTree(splitPath, key, false);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, true);
    verifyIfParentsContainPath(key, parents, true);

    key = new Path("file:///user/hive/warehouse/src");
    verifyIsPathWithInSubTree(splitPath, key, true);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, false);
    verifyIfParentsContainPath(key, parents, true);

    key = new Path("file:///user/hive/warehouse/src_2");
    verifyIsPathWithInSubTree(splitPath, key, false);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, false);
    verifyIfParentsContainPath(key, parents, false);

    key = new Path("file:///user/hive/warehouse/src/data.txt");
    verifyIsPathWithInSubTree(splitPath, key, true);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, false);
    verifyIfParentsContainPath(key, parents, true);
  }

  private void verifyIsPathWithInSubTree(Path splitPath, Path key, boolean expected) {
    boolean result = FileUtils.isPathWithinSubtree(splitPath, key);
    assertEquals("splitPath=" + splitPath + ", key=" + key, expected, result);
  }

  private void verifyIfParentsContainPath(Path key, Set<Path> parents, boolean expected) {
    boolean result = parents.contains(key);
    assertEquals("key=" + key, expected, result);
  }

  @Test
  public void testCopyWithDistcp() throws IOException {
    Path copySrc = new Path("copySrc");
    Path copyDst = new Path("copyDst");
    HiveConf conf = new HiveConf(TestFileUtils.class);

    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.getUri()).thenReturn(URI.create("hdfs:///"));

    ContentSummary mockContentSummary = mock(ContentSummary.class);
    when(mockContentSummary.getFileCount()).thenReturn(Long.MAX_VALUE);
    when(mockContentSummary.getLength()).thenReturn(Long.MAX_VALUE);
    when(mockFs.getContentSummary(any(Path.class))).thenReturn(mockContentSummary);

    HadoopShims shims = mock(HadoopShims.class);
    when(shims.runDistCp(Collections.singletonList(copySrc), copyDst, conf)).thenReturn(true);

    DataCopyStatistics copyStatistics = new DataCopyStatistics();
    Assert.assertTrue(FileUtils.copy(mockFs, copySrc, mockFs, copyDst, false, false, conf, shims, copyStatistics));
    verify(shims).runDistCp(Collections.singletonList(copySrc), copyDst, conf);
  }

  @Test
  public void testCopyWithDistCpAs() throws IOException {
    Path copySrc = new Path("copySrc");
    Path copyDst = new Path("copyDst");
    HiveConf conf = new HiveConf(TestFileUtils.class);

    FileSystem fs = copySrc.getFileSystem(conf);

    String doAsUser = conf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(
            doAsUser, UserGroupInformation.getLoginUser());

    HadoopShims shims = mock(HadoopShims.class);
    when(shims.runDistCpAs(Collections.singletonList(copySrc), copyDst, conf, proxyUser)).thenReturn(true);
    when(shims.runDistCp(Collections.singletonList(copySrc), copyDst, conf)).thenReturn(false);

    // doAs when asked
    Assert.assertTrue(FileUtils.distCp(fs, Collections.singletonList(copySrc), copyDst, false, proxyUser, conf, shims));
    verify(shims).runDistCpAs(Collections.singletonList(copySrc), copyDst, conf, proxyUser);
    // don't doAs when not asked
    Assert.assertFalse(FileUtils.distCp(fs, Collections.singletonList(copySrc), copyDst, true, null, conf, shims));
    verify(shims).runDistCp(Collections.singletonList(copySrc), copyDst, conf);

    // When distcp is done with doAs, the delete should also be done as doAs. But in current code its broken. This
    // should be fixed. For now check is added to avoid wrong usage. So if doAs is set, delete source should be false.
    try {
      FileUtils.distCp(fs, Collections.singletonList(copySrc), copyDst, true, proxyUser, conf, shims);
      Assert.assertTrue("Should throw IOException as doAs is called with delete source set to true".equals(""));
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().
              equalsIgnoreCase("Distcp is called with doAsUser and delete source set as true"));
    }
  }

  @Test
  public void testMakeRelative() {
    Path parentPath = new Path("/user/hive/database");
    Path childPath = new Path(parentPath, "table/dir/subdir");
    Path relativePath = FileUtils.makeRelative(parentPath, childPath);
    assertEquals("table/dir/subdir", relativePath.toString());

    // try with parent as Root.
    relativePath = FileUtils.makeRelative(new Path(Path.SEPARATOR), childPath);
    assertEquals("user/hive/database/table/dir/subdir", relativePath.toString());

    // try with non child path, it should return the child path as is.
    childPath = new Path("/user/hive/database1/table/dir/subdir");
    relativePath = FileUtils.makeRelative(parentPath, childPath);
    assertEquals(childPath.toString(), relativePath.toString());
  }

  @Test
  public void testListStatusIterator() throws Exception {
    MockFileSystem fs = new MockFileSystem(new HiveConf(),
        new MockFile("mock:/tmp/.staging", 500, new byte[0]),
        new MockFile("mock:/tmp/_dummy", 500, new byte[0]),
        new MockFile("mock:/tmp/dummy", 500, new byte[0]));
    Path path = new MockFileSystem.MockPath(fs, "/tmp");
    
    RemoteIterator<FileStatus> it = FileUtils.listStatusIterator(fs, path, FileUtils.HIDDEN_FILES_PATH_FILTER);
    assertEquals(1, assertExpectedFilePaths(it, Collections.singletonList("mock:/tmp/dummy")));
    
    RemoteIterator<LocatedFileStatus> itr = FileUtils.listFiles(fs, path, true, FileUtils.HIDDEN_FILES_PATH_FILTER);
    assertEquals(1, assertExpectedFilePaths(itr, Collections.singletonList("mock:/tmp/dummy")));
  }

  @Test
  public void testPathEscapeChars() {
    StringBuilder sb = new StringBuilder();
    FileUtils.charToEscape.stream().forEach(integer -> sb.append((char) integer));
    String path = sb.toString();
    assertEquals(path, FileUtils.unescapePathName(FileUtils.escapePathName(path)));
  }

  @Test
  public void testOzoneSameBucket() {
    assertTrue(FileUtils.isSameOzoneBucket(new Path("ofs://ozone1/vol1/bucket1/dir1"),
        new Path("ofs://ozone1/vol1/bucket1/dir2/file1")));
    assertTrue(FileUtils.isSameOzoneBucket(new Path("ofs://ozone1/vol1/bucket1/"),
        new Path("ofs://ozone1/vol1/bucket1/dir2/file1")));

    assertFalse(
        FileUtils.isSameOzoneBucket(new Path("ofs://ozone1/vol1/"), new Path("ofs://ozone1/vol1/bucket1/dir2/file1")));

    assertFalse(FileUtils.isSameOzoneBucket(new Path("ofs://ozone1/vol1/bucket1/"),
        new Path("ofs://ozone1/vol2/bucket1/dir2/file1")));

    assertFalse(FileUtils.isSameOzoneBucket(new Path("ofs://ozone1/vol1/bucket1/"),
        new Path("ofs://ozone1/vol1/bucket2/dir2/file1")));
  }

  private int assertExpectedFilePaths(RemoteIterator<? extends FileStatus> lfs, List<String> expectedPaths)
      throws Exception {
    int count = 0;
    while (lfs.hasNext()) {
      assertTrue(expectedPaths.contains(lfs.next().getPath().toString()));
      count++;
    }
    return count;
  }

  @Test
  public void testResolveSymlinks() throws IOException {
    HiveConf conf = new HiveConf();

    java.nio.file.Path original = java.nio.file.Files.createTempFile("", "");
    java.nio.file.Path symlinkPath = java.nio.file.Paths.get(original.toString() + ".symlink");
    java.nio.file.Path symlinkOfSymlinkPath = java.nio.file.Paths.get(original.toString() + ".symlink.symlink");

    // symlink -> original
    java.nio.file.Files.createSymbolicLink(symlinkPath, original);
    // symlink -> symlink -> original
    java.nio.file.Files.createSymbolicLink(symlinkOfSymlinkPath, symlinkPath);

    Assert.assertTrue(java.nio.file.Files.isSymbolicLink(symlinkPath));
    Assert.assertTrue(java.nio.file.Files.isSymbolicLink(symlinkOfSymlinkPath));

    // average usage 1: symlink points to the original
    Path originalPathResolved = FileUtils.resolveSymlinks(new Path(symlinkPath.toUri()), conf);
    Assert.assertEquals(original.toUri(), originalPathResolved.toUri());

    // average usage 2: symlink2 -> symlink -> original points to the original
    Path originalPathResolved2 = FileUtils.resolveSymlinks(new Path(symlinkOfSymlinkPath.toUri()), conf);
    Assert.assertEquals(original.toUri(), originalPathResolved2.toUri());

    // providing a symlink path without scheme: still resolving it as it was 'file' scheme
    // resolve to the original path then returning without scheme
    Path originalPathWithoutScheme = Path.getPathWithoutSchemeAndAuthority(new Path(original.toUri()));
    Path symlinkPathWithoutScheme = Path.getPathWithoutSchemeAndAuthority(new Path(symlinkPath.toUri()));
    Assert.assertNull(originalPathWithoutScheme.toUri().getScheme());
    Assert.assertNull(symlinkPathWithoutScheme.toUri().getScheme());

    Path originalPathResolvedWithoutInputScheme = FileUtils.resolveSymlinks(symlinkPathWithoutScheme, conf);
    // return path also hasn't got a scheme
    Assert.assertNull("Path without scheme should be resolved to another Path without scheme",
        originalPathResolvedWithoutInputScheme.toUri().getScheme());
    Assert.assertEquals(originalPathResolvedWithoutInputScheme, originalPathWithoutScheme);

    // a non-symlink is resolved to itself
    Path originalPathResolvedFromOriginal = FileUtils.resolveSymlinks(new Path(original.toUri()), conf);
    Assert.assertEquals(original.toUri(), originalPathResolvedFromOriginal.toUri());

    // 1. a nonexistent path is resolved to itself, resolveSymlinks doesn't care if the path doesn't exist
    // 2. a relative path without a scheme cannot be used to construct an URI, hence we get the input Path back
    Path nonexistentPath = new Path("./nonexistent-" + System.currentTimeMillis());
    Assert.assertEquals(nonexistentPath.toUri(), FileUtils.resolveSymlinks(nonexistentPath, conf).toUri());

    try {
      FileUtils.resolveSymlinks(null, conf);
      Assert.fail("IllegalArgumentException should be thrown in case of null input");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Cannot resolve symlink for a null Path", e.getMessage());
    }

    // hdfs is not supported, return safely with the original path
    Path hdfsPath = new Path("hdfs://localhost:0/user/hive/warehouse/src");
    Path resolvedHdfsPath = FileUtils.resolveSymlinks(hdfsPath, conf);
    Assert.assertEquals(hdfsPath.toUri(), resolvedHdfsPath.toUri());
  }
}
