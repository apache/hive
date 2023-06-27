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

package org.apache.hadoop.hive.metastore.leader;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestLeaderListener {

  @Test
  public void testAuditLeaderListener() throws Exception {
    Path location = new Path(".", "test_audit_leader_listener_" + System.currentTimeMillis());
    Configuration conf = MetastoreConf.newMetastoreConf();
    FileSystem fileSystem = FileSystem.get(conf);
    try {
      MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_NEW_AUDIT_FILE, true);
      MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_AUDIT_FILE_LIMIT, 3);
      // create a new file on the election event
      AuditLeaderListener listener = new AuditLeaderListener(location, conf);
      StaticLeaderElection election = new StaticLeaderElection();
      election.setName("testAuditLeaderListener");
      listener.takeLeadership(election);
      List<FileStatus> fileStatuses = FileUtils.getFileStatusRecurse(location, fileSystem);
      Pattern pattern = Pattern.compile("leader_testAuditLeaderListener_[0-9]+\\.json");
      fileStatuses.forEach(fileStatus -> Assert.assertTrue(pattern.matcher(fileStatus.getPath().getName()).matches()));

      FileStatus oldestFile = fileStatuses.get(0);
      listener.takeLeadership(election);
      fileStatuses = FileUtils.getFileStatusRecurse(location, fileSystem);
      Assert.assertTrue(fileStatuses.remove(oldestFile));
      Assert.assertTrue(fileStatuses.size() == 1);
      FileStatus tmpFileStatus = fileStatuses.get(0);

      listener.takeLeadership(election);
      fileStatuses = FileUtils.getFileStatusRecurse(location, fileSystem);
      Assert.assertTrue(fileStatuses.size() == 3);
      List<FileStatus> tempFileStatuses = new ArrayList<>(fileStatuses);
      fileStatuses.forEach(fileStatus -> Assert.assertTrue(pattern.matcher(fileStatus.getPath().getName()).matches()));

      listener.takeLeadership(election);
      fileStatuses = FileUtils.getFileStatusRecurse(location, fileSystem);
      Assert.assertTrue(fileStatuses.size() == 3);
      Assert.assertTrue(tempFileStatuses.remove(oldestFile));
      Assert.assertTrue(tempFileStatuses.contains(tmpFileStatus));
      Assert.assertFalse(fileStatuses.contains(oldestFile));
      tempFileStatuses.removeAll(fileStatuses);
      Assert.assertTrue(tempFileStatuses.isEmpty());

      listener.takeLeadership(election);
      fileStatuses = FileUtils.getFileStatusRecurse(location, fileSystem);
      Assert.assertTrue(fileStatuses.size() == 3);
      Assert.assertFalse(fileStatuses.contains(tmpFileStatus));

      // only one file
      fileSystem.delete(location, true);
      Assert.assertTrue(FileUtils.isDirEmpty(fileSystem, location));
      MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_NEW_AUDIT_FILE, false);
      listener.takeLeadership(election);
      fileStatuses = FileUtils.getFileStatusRecurse(location, fileSystem);
      Assert.assertTrue(fileStatuses.size() == 1);
      Assert.assertTrue(fileStatuses.get(0).getPath().getName().equals("leader_testAuditLeaderListener.json"));
      listener.takeLeadership(election);
      listener.takeLeadership(election);
      fileStatuses = FileUtils.getFileStatusRecurse(location, fileSystem);
      Assert.assertTrue(fileStatuses.size() == 1);
      Assert.assertTrue(fileStatuses.get(0).getPath().getName().equals("leader_testAuditLeaderListener.json"));
    } finally {
      FileUtils.moveToTrash(fileSystem, location, conf, true);
    }
  }

}
