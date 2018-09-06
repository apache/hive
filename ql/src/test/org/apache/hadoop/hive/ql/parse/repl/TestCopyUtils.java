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
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Unit Test class for CopyUtils class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ CopyUtils.class, FileUtils.class, Utils.class, UserGroupInformation.class})
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
    CopyUtils copyUtils = new CopyUtils("", conf);
    long MB_128 = 128 * 1024 * 1024;
    assertFalse(copyUtils.limitReachedForLocalCopy(MB_128, 1L));
  }

  @Test
  public void distcpShouldNotBeCalledForSmallerFileSize() throws Exception {
    mockStatic(UserGroupInformation.class);
    when(UserGroupInformation.getCurrentUser()).thenReturn(mock(UserGroupInformation.class));

    HiveConf conf = Mockito.spy(new HiveConf());
    CopyUtils copyUtils = new CopyUtils("", conf);
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
    CopyUtils copyUtils = Mockito.spy(new CopyUtils(null, conf));

    mockStatic(FileUtils.class);
    mockStatic(Utils.class);
    when(destination.getFileSystem(same(conf))).thenReturn(fs);
    when(source.getFileSystem(same(conf))).thenReturn(fs);
    when(FileUtils.distCp(same(fs), anyListOf(Path.class), same(destination),
                          anyBoolean(), eq(null), same(conf),
                          same(ShimLoader.getHadoopShims())))
        .thenReturn(false);
    when(Utils.getUGI()).thenReturn(mock(UserGroupInformation.class));
    doReturn(false).when(copyUtils).regularCopy(same(fs), same(fs), anyListOf(ReplChangeManager.FileInfo.class));

    copyUtils.doCopy(destination, srcPaths);
  }
}