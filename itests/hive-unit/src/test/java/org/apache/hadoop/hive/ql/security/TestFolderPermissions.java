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
package org.apache.hadoop.hive.ql.security;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFolderPermissions extends FolderPermissionBase {

  @BeforeClass
  public static void setup() throws Exception {
    conf = new HiveConf(TestFolderPermissions.class);
    conf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    baseSetup();
  }

  public FsPermission[] expected = new FsPermission[] {
     FsPermission.createImmutable((short) 0777),
     FsPermission.createImmutable((short) 0766)
  };

  @Override
  public void setPermission(String locn, int permIndex) throws Exception {
    fs.setPermission(new Path(locn), expected[permIndex]);
  }

  @Override
  public void verifyPermission(String locn, int permIndex) throws Exception {
    FsPermission actual =  fs.getFileStatus(new Path(locn)).getPermission();
    Assert.assertEquals(expected[permIndex], actual);
  }
}
