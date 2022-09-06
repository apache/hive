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

package org.apache.hadoop.hive.ql.parse.repl;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Unit Test class for CopyUtils class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ CopyUtils.class, FileUtils.class, Utils.class, UserGroupInformation.class, ReplChangeManager.class})
@PowerMockIgnore({ "javax.management.*" })
public class TestCopyUtils {
  /*
  Distcp currently does not copy a single file in a distributed manner hence we dont care about
  the size of file, if there is only file, we dont want to launch distcp.
   */
  @Test
  public void distcpShouldNotBeCalledOnlyForOneFile() throws Exception {
    mockStatic(UserGroupInformation.class);
    when(UserGroupInformation.getCurrentUser()).thenReturn(mock(UserGroupInformation.class));

    HiveConf conf = Mockito.spy(new HiveConf());
    doReturn(1L).when(conf).getLong(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname, 32L * 1024 * 1024);
    CopyUtils copyUtils = new CopyUtils("", conf, null);
    long MB_128 = 128 * 1024 * 1024;
    assertFalse(copyUtils.limitReachedForLocalCopy(MB_128, 1L));
  }

  @Test
  public void distcpShouldNotBeCalledForSmallerFileSize() throws Exception {
    mockStatic(UserGroupInformation.class);
    when(UserGroupInformation.getCurrentUser()).thenReturn(mock(UserGroupInformation.class));

    HiveConf conf = Mockito.spy(new HiveConf());
    CopyUtils copyUtils = new CopyUtils("", conf, null);
    long MB_16 = 16 * 1024 * 1024;
    assertFalse(copyUtils.limitReachedForLocalCopy(MB_16, 100L));
  }

  @Test(expected = IOException.class)
  public void shouldThrowExceptionOnDistcpFailure() throws Exception {
    Path destination = mock(Path.class);
    Path source = mock(Path.class);
    FileSystem fs = mock(FileSystem.class);
    List<Path> srcPaths = Arrays.asList(source, source);
    HiveConf conf = mock(HiveConf.class);
    CopyUtils copyUtils = Mockito.spy(new CopyUtils(null, conf, fs));

    mockStatic(FileUtils.class);
    mockStatic(Utils.class);
    when(destination.getFileSystem(same(conf))).thenReturn(fs);
    when(source.getFileSystem(same(conf))).thenReturn(fs);
    when(FileUtils.distCp(same(fs), anyListOf(Path.class), same(destination),
                          anyBoolean(), eq(null), same(conf),
                          same(ShimLoader.getHadoopShims())))
        .thenReturn(false);
    when(Utils.getUGI()).thenReturn(mock(UserGroupInformation.class));
    doReturn(false).when(copyUtils).regularCopy(same(fs), anyListOf(ReplChangeManager.FileInfo.class));

    copyUtils.doCopy(destination, srcPaths);
  }

  @Test
  public void testFSCallsFailOnParentExceptions() throws Exception {
    mockStatic(UserGroupInformation.class);
    mockStatic(ReplChangeManager.class);
    when(UserGroupInformation.getCurrentUser()).thenReturn(mock(UserGroupInformation.class));
    HiveConf conf = mock(HiveConf.class);
    conf.set(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY.varname, "1s");
    FileSystem fs = mock(FileSystem.class);
    Path source = mock(Path.class);
    Path destination = mock(Path.class);
    ContentSummary cs = mock(ContentSummary.class);

    Exception exception = new org.apache.hadoop.fs.PathPermissionException("Failed");
    when(ReplChangeManager.checksumFor(source, fs)).thenThrow(exception).thenReturn("dummy");
    when(fs.exists(same(source))).thenThrow(exception).thenReturn(true);
    when(fs.delete(same(source), anyBoolean())).thenThrow(exception).thenReturn(true);
    when(fs.mkdirs(same(source))).thenThrow(exception).thenReturn(true);
    when(fs.rename(same(source), same(destination))).thenThrow(exception).thenReturn(true);
    when(fs.getContentSummary(same(source))).thenThrow(exception).thenReturn(cs);

    CopyUtils copyUtils = new CopyUtils(UserGroupInformation.getCurrentUser().getUserName(), conf, fs);
    CopyUtils copyUtilsSpy = Mockito.spy(copyUtils);
    try {
      copyUtilsSpy.exists(fs, source);
    } catch (Exception e) {
      assertEquals(exception.getClass(), e.getCause().getClass());
    }
    Mockito.verify(fs, Mockito.times(1)).exists(source);
    try {
      copyUtils.delete(fs, source, true);
    } catch (Exception e) {
      assertEquals(exception.getClass(), e.getCause().getClass());
    }
    Mockito.verify(fs, Mockito.times(1)).delete(source, true);
    try {
      copyUtils.mkdirs(fs, source);
    } catch (Exception e) {
      assertEquals(exception.getClass(), e.getCause().getClass());
    }
    Mockito.verify(fs, Mockito.times(1)).mkdirs(source);
    try {
      copyUtils.rename(fs, source, destination);
    } catch (Exception e) {
      assertEquals(exception.getClass(), e.getCause().getClass());
    }
    Mockito.verify(fs, Mockito.times(1)).rename(source, destination);
    try {
      copyUtilsSpy.getContentSummary(fs, source);
    } catch (Exception e) {
      assertEquals(exception.getClass(), e.getCause().getClass());;
    }
    Mockito.verify(fs, Mockito.times(1)).getContentSummary(source);
    try {
      copyUtilsSpy.checkSumFor(source, fs);
    } catch (Exception e) {
      assertEquals(exception.getClass(), e.getCause().getClass());
    }
    Mockito.verify(copyUtilsSpy, Mockito.times(1)).checkSumFor(source, fs);
  }

  @Test
  public void testRetryableFSCalls() throws Exception {
    mockStatic(UserGroupInformation.class);
    mockStatic(ReplChangeManager.class);
    when(UserGroupInformation.getCurrentUser()).thenReturn(mock(UserGroupInformation.class));
    HiveConf conf = mock(HiveConf.class);
    conf.set(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY.varname, "1s");
    FileSystem fs = mock(FileSystem.class);
    Path source = mock(Path.class);
    Path destination = mock(Path.class);
    ContentSummary cs = mock(ContentSummary.class);

    when(ReplChangeManager.checksumFor(source, fs)).thenThrow(new IOException("Failed")).thenReturn("dummy");
    when(fs.exists(same(source))).thenThrow(new IOException("Failed")).thenReturn(true);
    when(fs.delete(same(source), anyBoolean())).thenThrow(new IOException("Failed")).thenReturn(true);
    when(fs.mkdirs(same(source))).thenThrow(new IOException("Failed")).thenReturn(true);
    when(fs.rename(same(source), same(destination))).thenThrow(new IOException("Failed")).thenReturn(true);
    when(fs.getContentSummary(same(source))).thenThrow(new IOException("Failed")).thenReturn(cs);

    CopyUtils copyUtils = new CopyUtils(UserGroupInformation.getCurrentUser().getUserName(), conf, fs);
    CopyUtils copyUtilsSpy = Mockito.spy(copyUtils);
    assertEquals (true, copyUtilsSpy.exists(fs, source));
    Mockito.verify(fs, Mockito.times(2)).exists(source);
    assertEquals (true, copyUtils.delete(fs, source, true));
    Mockito.verify(fs, Mockito.times(2)).delete(source, true);
    assertEquals (true, copyUtils.mkdirs(fs, source));
    Mockito.verify(fs, Mockito.times(2)).mkdirs(source);
    assertEquals (true, copyUtils.rename(fs, source, destination));
    Mockito.verify(fs, Mockito.times(2)).rename(source, destination);
    assertEquals (cs, copyUtilsSpy.getContentSummary(fs, source));
    Mockito.verify(fs, Mockito.times(2)).getContentSummary(source);
    assertEquals ("dummy", copyUtilsSpy.checkSumFor(source, fs));
  }

  @Test
  public void testParallelCopySuccess() throws Exception {
    mockStatic(UserGroupInformation.class);
    when(UserGroupInformation.getCurrentUser()).thenReturn(mock(UserGroupInformation.class));
    HiveConf conf = Mockito.spy(new HiveConf());
    when(conf.getIntVar(HiveConf.ConfVars.REPL_PARALLEL_COPY_TASKS)).thenReturn(2);
    when(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL)).thenReturn(true);
    FileSystem destFs = mock(FileSystem.class);
    when(destFs.exists(Mockito.any())).thenReturn(true);
    CopyUtils copyUtils = new CopyUtils(UserGroupInformation.getCurrentUser().getUserName(), conf, destFs);
    CopyUtils copyUtilsSpy = Mockito.spy(copyUtils);
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    ExecutorService mockExecutorService = Mockito.spy(executorService);
    when(copyUtilsSpy.getExecutorService()).thenReturn(mockExecutorService);
    Path destination = new Path("dest");
    Path source = mock(Path.class);
    FileSystem fs = mock(FileSystem.class);
    ReplChangeManager.FileInfo srcFileInfo = new ReplChangeManager.FileInfo(fs, source, "path1");
    List<ReplChangeManager.FileInfo> srcFiles = Arrays.asList(srcFileInfo);
    doNothing().when(copyUtilsSpy).doCopy(Mockito.any(), Mockito.any(),
      Mockito.anyBoolean(), Mockito.anyBoolean());
    copyUtilsSpy.copyAndVerify(destination, srcFiles, source, true, true);
    Class<Collection<? extends Callable<Void>>> listClass =
      (Class<Collection<? extends Callable<Void>>>)(Class)List.class;
    //Thread pool Not invoked as only one target path
    ArgumentCaptor<Collection<? extends Callable<Void>>> callableCapture = ArgumentCaptor.forClass(listClass);
    Mockito.verify(mockExecutorService, Mockito.times(0)).invokeAll(callableCapture.capture());
    ReplChangeManager.FileInfo srcFileInfo1 = new ReplChangeManager.FileInfo(fs, source, "path2");
    ReplChangeManager.FileInfo srcFileInfo2 = new ReplChangeManager.FileInfo(fs, source, "path3");
    srcFiles = Arrays.asList(srcFileInfo1, srcFileInfo2);
    executorService = Executors.newFixedThreadPool(2);
    mockExecutorService = Mockito.spy(executorService);
    when(copyUtilsSpy.getExecutorService()).thenReturn(mockExecutorService);
    copyUtilsSpy.copyAndVerify(destination, srcFiles, source, true, true);
    //File count is greater than 1 do thread pool invoked
    Mockito.verify(mockExecutorService,
      Mockito.times(1)).invokeAll(callableCapture.capture());
  }

  @Test
  public void testLeaveIdenticalFilesOnly() throws IOException {
    // root
    Path root = new Path("a").getParent();
    FileStatus rootStatus = mock(FileStatus.class);
    when(rootStatus.getPath()).thenReturn(root);
    when(rootStatus.isDirectory()).thenReturn(true);

    // a/
    Path a = new Path("a");
    FileStatus aStatus = mock(FileStatus.class);
    when(aStatus.getPath()).thenReturn(a);
    when(aStatus.isDirectory()).thenReturn(true);

    // a/A
    Path aA = new Path("a", "A");
    FileStatus aAStatus = mock(FileStatus.class);
    when(aAStatus.getPath()).thenReturn(aA);
    when(aAStatus.isFile()).thenReturn(true);

    // a/B
    Path aB = new Path("a", "B");
    FileStatus aBStatus = mock(FileStatus.class);
    when(aBStatus.getPath()).thenReturn(aB);
    when(aBStatus.isFile()).thenReturn(true);

    // b/
    Path b = new Path("b");
    FileStatus bStatus = mock(FileStatus.class);
    when(bStatus.getPath()).thenReturn(b);
    when(bStatus.isDirectory()).thenReturn(true);

    // b/C
    Path bC = new Path("b", "C");
    FileStatus bCStatus = mock(FileStatus.class);
    when(bCStatus.getPath()).thenReturn(bC);
    when(bCStatus.isFile()).thenReturn(true);

    // b/D
    Path bD = new Path("b", "D");
    FileStatus bDStatus = mock(FileStatus.class);
    when(bDStatus.getPath()).thenReturn(bD);
    when(bDStatus.isFile()).thenReturn(true);

    // sourceFs
    FileSystem sourceFs = mock(FileSystem.class);
    FileChecksum checksum = mock(FileChecksum.class);
    when(sourceFs.exists(isOneOf(root, a, b, aA, aB, bC))).thenReturn(true);
    when(sourceFs.getScheme()).thenReturn("hdfs");
    when(sourceFs.getFileChecksum(any())).thenReturn(checksum);

    // source structure: root(a(A, B), b(C))
    when(sourceFs.listStatus(root)).thenReturn(new FileStatus[] {aStatus, bStatus});
    when(sourceFs.listStatus(a)).thenReturn(new FileStatus[] {aAStatus, aBStatus});
    when(sourceFs.getFileStatus(root)).thenReturn(rootStatus);
    when(sourceFs.getFileStatus(a)).thenReturn(aStatus);
    when(sourceFs.getFileStatus(aA)).thenReturn(aAStatus);
    when(sourceFs.getFileStatus(aB)).thenReturn(aBStatus);
    when(sourceFs.listStatus(b)).thenReturn(new FileStatus[] {bCStatus});
    when(sourceFs.getFileStatus(b)).thenReturn(bStatus);
    when(sourceFs.getFileStatus(bC)).thenReturn(bCStatus);

    // destinationFs
    FileSystem destinationFs = mock(FileSystem.class);
    when(destinationFs.exists(isOneOf(root, a, b, aA, bD))).thenReturn(true);
    when(destinationFs.getScheme()).thenReturn("hdfs");
    when(destinationFs.getFileChecksum(any())).thenReturn(checksum);

    // destination structure: root(a(A), b(D))
    when(destinationFs.listStatus(root)).thenReturn(new FileStatus[] {aStatus, bStatus});
    when(destinationFs.listStatus(a)).thenReturn(new FileStatus[] {aAStatus});
    when(destinationFs.getFileStatus(root)).thenReturn(rootStatus);
    when(destinationFs.getFileStatus(a)).thenReturn(aStatus);
    when(destinationFs.getFileStatus(aA)).thenReturn(aAStatus);
    when(destinationFs.listStatus(b)).thenReturn(new FileStatus[] {bDStatus});
    when(destinationFs.getFileStatus(b)).thenReturn(bStatus);
    when(destinationFs.getFileStatus(bD)).thenReturn(bDStatus);

    // Leave identical files only.
    // The destination will leave root(a(A)) only, and b(D) will be deleted.
    CopyUtils copyUtils = new CopyUtils("", new HiveConf(), destinationFs);
    copyUtils.leaveIdenticalFilesOnly(
        sourceFs, new Path[] {root},
        destinationFs, root
    );

    // b/D should be deleted because it's not in the source.
    verify(destinationFs, times(1)).delete(bD, true);

    // Other files and directories should not be deleted.
    verify(destinationFs, never()).delete(eq(a), anyBoolean());
    verify(destinationFs, never()).delete(eq(aA), anyBoolean());
    verify(destinationFs, never()).delete(eq(aB), anyBoolean());
    verify(destinationFs, never()).delete(eq(b), anyBoolean());
    verify(destinationFs, never()).delete(eq(bC), anyBoolean());
  }

  @SafeVarargs
  private static <T> T isOneOf(T ... args) {
    return Mockito.argThat(argument -> Arrays.asList(args).contains(argument));
  }
}
