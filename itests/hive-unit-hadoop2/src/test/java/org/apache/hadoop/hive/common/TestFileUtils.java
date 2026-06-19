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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.DataCopyStatistics;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for {{@link FileUtils}. Tests run against a {@link HadoopShims.MiniDFSShim}.
 */
public class TestFileUtils {

  private static final Path basePath = new Path("/tmp/");

  private static HiveConf conf;
  private static FileSystem fs;
  private static HadoopShims.MiniDFSShim dfs;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new HiveConf(TestFileUtils.class);
    dfs = ShimLoader.getHadoopShims().getMiniDfs(conf, 4, true, null);
    fs = dfs.getFileSystem();
  }

  @Test
  public void testCopySingleEmptyFile() throws IOException {
    String file1Name = "file1.txt";
    Path copySrc = new Path(basePath, "copySrc");
    Path copyDst = new Path(basePath, "copyDst");
    try {
      fs.create(new Path(basePath, new Path(copySrc, file1Name))).close();
      DataCopyStatistics copyStatistics = new DataCopyStatistics();
      Assert.assertTrue("FileUtils.copy failed to copy data",
              FileUtils.copy(fs, copySrc, fs, copyDst, false, false, conf, copyStatistics));

      Path dstFileName1 = new Path(copyDst, file1Name);
      Assert.assertTrue(fs.exists(new Path(copyDst, file1Name)));
      Assert.assertEquals(fs.getFileStatus(dstFileName1).getLen(), 0);
    } finally {
      try {
        fs.delete(copySrc, true);
        fs.delete(copyDst, true);
      } catch (IOException e) {
        // Do nothing
      }
    }
  }

  @Test
  public void testXAttrsPreserved() throws Exception {
    //Case 1) src and dst are files.
    Path src = new Path(basePath, "src.txt");
    fs.create(src).close();
    setXAttrsRecursive(src);
    Path dst = new Path(basePath, "dst.txt");
    Assert.assertFalse(fs.exists(dst));
    DataCopyStatistics copyStatistics = new DataCopyStatistics();
    Assert.assertTrue(FileUtils.doIOUtilsCopyBytes(fs, fs.getFileStatus(src), fs, dst, false, true, true, conf, copyStatistics));
    Assert.assertTrue(fs.exists(dst));
    verifyXAttrsPreserved(src, dst);
    //Case 2) src is file and dst directory does not exist.
    dst = new Path(basePath, "dummyDstDir");
    Assert.assertFalse(fs.exists(dst));
    copyStatistics = new DataCopyStatistics();
    Assert.assertTrue(FileUtils.doIOUtilsCopyBytes(fs, fs.getFileStatus(src), fs, dst, false, true, true, conf, copyStatistics));
    Assert.assertTrue(fs.exists(dst));
    Assert.assertTrue(fs.exists(new Path(dst, new Path(basePath, "src.txt"))));
    verifyXAttrsPreserved(src, dst);
    //Case 3) src is a file and dst directory exists.
    dst = new Path(basePath, "dummyDstDir1");
    fs.mkdirs(dst);
    Assert.assertTrue(fs.exists(dst));
    copyStatistics = new DataCopyStatistics();
    Assert.assertTrue(FileUtils.doIOUtilsCopyBytes(fs, fs.getFileStatus(src), fs, dst, false, true, true, conf, copyStatistics));
    Assert.assertTrue(fs.exists(dst));
    Assert.assertTrue(fs.exists(new Path(dst, "src.txt")));
    verifyXAttrsPreserved(src, new Path(dst, "src.txt"));
    //Case 4) src & dst are directories and dst does not exist.
    src = new Path(basePath, "dummySrcDir2");
    dst = new Path(basePath, "dummyDstDir2");
    fs.create(new Path(src, "src.txt"));
    setXAttrsRecursive(src);
    Assert.assertFalse(fs.exists(dst));
    copyStatistics = new DataCopyStatistics();
    Assert.assertTrue(FileUtils.doIOUtilsCopyBytes(fs, fs.getFileStatus(src), fs, dst, false, true, true, conf, copyStatistics));
    Assert.assertTrue(fs.exists(dst));
    Assert.assertTrue(fs.exists(new Path(dst, "src.txt")));
    verifyXAttrsPreserved(src, dst);
    //Case 5) src & dst are directories and dst directory exists
    src = new Path(basePath, "dummySrcDir3");
    dst = new Path(basePath, "dummyDstDir3");
    fs.create(new Path(src, "src.txt"));
    fs.mkdirs(dst);
    setXAttrsRecursive(src);
    Assert.assertTrue(fs.exists(dst));
    copyStatistics = new DataCopyStatistics();
    Assert.assertTrue(FileUtils.doIOUtilsCopyBytes(fs, fs.getFileStatus(src), fs, dst, false, true, true, conf, copyStatistics));
    Assert.assertTrue(fs.exists(new Path(dst, "dummySrcDir3/src.txt")));
    verifyXAttrsPreserved(src, new Path(dst, src.getName()));
  }

  @Test
  public void testShouldPreserveXAttrs() throws Exception {
    conf.setBoolean(HiveConf.ConfVars.DFS_XATTR_ONLY_SUPPORTED_ON_RESERVED_NAMESPACE.varname, true);
    Path filePath = new Path(basePath, "src.txt");
    fs.create(filePath).close();
    Assert.assertFalse(FileUtils.shouldPreserveXAttrs(conf, fs, fs, filePath));
    Path reservedRawPath = new Path("/.reserved/raw/", "src1.txt");
    fs.create(reservedRawPath).close();
    Assert.assertTrue(FileUtils.shouldPreserveXAttrs(conf, fs, fs, reservedRawPath));

    conf.setBoolean(HiveConf.ConfVars.DFS_XATTR_ONLY_SUPPORTED_ON_RESERVED_NAMESPACE.varname, false);
    Assert.assertTrue(FileUtils.shouldPreserveXAttrs(conf, fs, fs, filePath));
    Assert.assertTrue(FileUtils.shouldPreserveXAttrs(conf, fs, fs, reservedRawPath));
  }

  private void verifyXAttrsPreserved(Path src, Path dst) throws Exception {
    FileStatus srcStatus = fs.getFileStatus(src);
    FileStatus dstStatus = fs.getFileStatus(dst);
    if (srcStatus.isDirectory()) {
      Assert.assertTrue(dstStatus.isDirectory());
      for(FileStatus srcContent: fs.listStatus(src)) {
        Path dstContent = new Path(dst, srcContent.getPath().getName());
        Assert.assertTrue(fs.exists(dstContent));
        verifyXAttrsPreserved(srcContent.getPath(), dstContent);
      }
    } else {
      Assert.assertFalse(dstStatus.isDirectory());
    }
    Map<String, byte[]> values = fs.getXAttrs(dst);
    for(Map.Entry<String, byte[]> value : fs.getXAttrs(src).entrySet()) {
      Assert.assertEquals(new String(value.getValue()), new String(values.get(value.getKey())));
    }
  }

  private void setXAttrsRecursive(Path path) throws Exception {
    if (fs.getFileStatus(path).isDirectory()) {
      RemoteIterator<FileStatus> content = fs.listStatusIterator(path);
      while(content.hasNext()) {
        setXAttrsRecursive(content.next().getPath());
      }
    }
    fs.setXAttr(path, "user.random", "value".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testCopyWithDistcp() throws IOException {
    String file1Name = "file1.txt";
    String file2Name = "file2.txt";
    Path copySrc = new Path(basePath, "copySrc");
    Path copyDst = new Path(basePath, "copyDst");
    Path srcFile1 = new Path(basePath, new Path(copySrc, file1Name));
    Path srcFile2 = new Path(basePath, new Path(copySrc, file2Name));
    try {
      OutputStream os1 = fs.create(srcFile1);
      os1.write(new byte[]{1, 2, 3});
      os1.close();

      OutputStream os2 = fs.create(srcFile2);
      os2.write(new byte[]{1, 2, 3});
      os2.close();

      conf.set(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname, "1");
      conf.set(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname, "1");
      DataCopyStatistics copyStatistics = new DataCopyStatistics();
      Assert.assertTrue("FileUtils.copy failed to copy data",
              FileUtils.copy(fs, copySrc, fs, copyDst, false, false, conf, copyStatistics));
      Assert.assertEquals(6, copyStatistics.getBytesCopied());

      Path dstFileName1 = new Path(copyDst, file1Name);
      Assert.assertTrue(fs.exists(new Path(copyDst, file1Name)));
      Assert.assertEquals(fs.getFileStatus(dstFileName1).getLen(), 3);

      Path dstFileName2 = new Path(copyDst, file2Name);
      Assert.assertTrue(fs.exists(new Path(copyDst, file2Name)));
      Assert.assertEquals(fs.getFileStatus(dstFileName2).getLen(), 3);
    } finally {
      try {
        fs.delete(copySrc, true);
        fs.delete(copyDst, true);
      } catch (IOException e) {
        // Do nothing
      }
    }
  }

  @AfterClass
  public static void shutdown() throws IOException {
    fs.close();
    dfs.shutdown();
  }
}
