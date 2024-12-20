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
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit Test class for CopyUtils class.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestCopyUtils {
  /*
  Distcp currently does not copy a single file in a distributed manner hence we dont care about
  the size of file, if there is only file, we dont want to launch distcp.
   */
  @Test
  public void distcpShouldNotBeCalledOnlyForOneFile() throws Exception {
    try (MockedStatic<UserGroupInformation> userGroupInformationMockedStatic = mockStatic(UserGroupInformation.class)) {
      userGroupInformationMockedStatic.when(UserGroupInformation::getCurrentUser).thenReturn(mock(UserGroupInformation.class));

      HiveConf conf = Mockito.spy(new HiveConf());
      doReturn(1L).when(conf).getLong(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname, 32L * 1024 * 1024);
      CopyUtils copyUtils = new CopyUtils("", conf, null);
      long MB_128 = 128 * 1024 * 1024;
      assertFalse(copyUtils.limitReachedForLocalCopy(MB_128, 1L));
    }
  }

  @Test
  public void distcpShouldNotBeCalledForSmallerFileSize() throws Exception {
    try (MockedStatic<UserGroupInformation> userGroupInformationMockedStatic = mockStatic(UserGroupInformation.class)) {
      userGroupInformationMockedStatic.when(UserGroupInformation::getCurrentUser).thenReturn(mock(UserGroupInformation.class));

      HiveConf conf = Mockito.spy(new HiveConf());
      CopyUtils copyUtils = new CopyUtils("", conf, null);
      long MB_16 = 16 * 1024 * 1024;
      assertFalse(copyUtils.limitReachedForLocalCopy(MB_16, 100L));
    }
  }

  @Test(expected = IOException.class)
  public void shouldThrowExceptionOnDistcpFailure() throws Exception {
    Path destination = mock(Path.class);
    Path source = mock(Path.class);
    FileSystem fs = mock(FileSystem.class);
    List<Path> srcPaths = Arrays.asList(source, source);
    HiveConf conf = mock(HiveConf.class);
    CopyUtils copyUtils = Mockito.spy(new CopyUtils(null, conf, fs));
    doReturn(false).when(copyUtils).regularCopy(same(fs), anyList());

    when(source.getFileSystem(same(conf))).thenReturn(fs);
    try (MockedStatic<FileUtils> fileUtilsMockedStatic = mockStatic(FileUtils.class);
         MockedStatic<Utils> utilsMockedStatic = mockStatic(Utils.class)) {
      fileUtilsMockedStatic.when(
              () -> FileUtils.distCp(same(fs), anyList(), same(destination), anyBoolean(), eq(null), same(conf),
                      same(ShimLoader.getHadoopShims()))).thenReturn(false);
      utilsMockedStatic.when(Utils::getUGI).thenReturn(mock(UserGroupInformation.class));

      copyUtils.doCopy(destination, srcPaths);
    }
  }

  @Test
  public void testFSCallsFailOnParentExceptions() throws Exception {
    try (MockedStatic<UserGroupInformation> userGroupInformationMockedStatic = mockStatic(UserGroupInformation.class);
         MockedStatic<ReplChangeManager> replChangeManagerMockedStatic = mockStatic(ReplChangeManager.class)) {
      userGroupInformationMockedStatic.when(UserGroupInformation::getCurrentUser).thenReturn(mock(UserGroupInformation.class));
      HiveConf conf = mock(HiveConf.class);
      conf.set(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY.varname, "1s");
      FileSystem fs = mock(FileSystem.class);
      Path source = mock(Path.class);
      Path destination = mock(Path.class);
      ContentSummary cs = mock(ContentSummary.class);

      Exception exception = new org.apache.hadoop.fs.PathPermissionException("Failed");
      replChangeManagerMockedStatic.when(() -> ReplChangeManager.checksumFor(source, fs)).thenThrow(exception).thenReturn("dummy");
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
  }

  @Test
  public void testRetryableFSCalls() throws Exception {
    try (MockedStatic<UserGroupInformation> userGroupInformationMockedStatic = mockStatic(UserGroupInformation.class);
         MockedStatic<ReplChangeManager> replChangeManagerMockedStatic = mockStatic(ReplChangeManager.class)) {
      userGroupInformationMockedStatic.when(UserGroupInformation::getCurrentUser).thenReturn(mock(UserGroupInformation.class));
      HiveConf conf = mock(HiveConf.class);
      conf.set(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY.varname, "1s");
      FileSystem fs = mock(FileSystem.class);
      Path source = mock(Path.class);
      Path destination = mock(Path.class);
      ContentSummary cs = mock(ContentSummary.class);

      replChangeManagerMockedStatic.when(() -> ReplChangeManager.checksumFor(source, fs)).thenThrow(new IOException("Failed")).thenReturn("dummy");
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
  }

  @Test
  public void testParallelCopySuccess() throws Exception {
    try (MockedStatic<UserGroupInformation> userGroupInformationMockedStatic =  mockStatic(UserGroupInformation.class)) {
      userGroupInformationMockedStatic.when(UserGroupInformation::getCurrentUser).thenReturn(mock(UserGroupInformation.class));
      HiveConf conf = Mockito.spy(new HiveConf());
      when(conf.getIntVar(HiveConf.ConfVars.REPL_PARALLEL_COPY_TASKS)).thenReturn(2);
      when(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL)).thenReturn(true);
      FileSystem destFs = mock(FileSystem.class);
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
        anyBoolean(), anyBoolean(), Mockito.any());
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
  }
}
