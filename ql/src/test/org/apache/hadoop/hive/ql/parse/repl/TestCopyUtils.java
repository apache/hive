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

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class TestCopyUtils {
  /*
  Distcp currently does not copy a single file in a distributed manner hence we dont care about
  the size of file, if there is only file, we dont want to launch distcp.
   */
  @Test
  public void distcpShouldNotBeCalledOnlyForOneFile() {
    HiveConf conf = new HiveConf();
    conf.setLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE, 1);
    CopyUtils copyUtils = new CopyUtils("", conf);
    long MB_128 = 128 * 1024 * 1024;
    assertFalse(copyUtils.limitReachedForLocalCopy(MB_128, 1L));
  }

  @Test
  public void distcpShouldNotBeCalledForSmallerFileSize() {
    HiveConf conf = new HiveConf();
    CopyUtils copyUtils = new CopyUtils("", conf);
    long MB_16 = 16 * 1024 * 1024;
    assertFalse(copyUtils.limitReachedForLocalCopy(MB_16, 100L));
  }
}