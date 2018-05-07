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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.shims.HadoopShims.WebHCatJTShim;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.templeton.tool.JobState;

/**
 * Delete a job
 */
public class DeleteDelegator extends TempletonDelegator {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteDelegator.class);
  public DeleteDelegator(AppConfig appConf) {
    super(appConf);
  }

  public QueueStatusBean run(String user, String id)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException
  {
    UserGroupInformation ugi = null;
    WebHCatJTShim tracker = null;
    JobState state = null;
    try {
      ugi = UgiFactory.getUgi(user);
      tracker = ShimLoader.getHadoopShims().getWebHCatShim(appConf, ugi);
      JobID jobid = StatusDelegator.StringToJobID(id);
      if (jobid == null)
        throw new BadParam("Invalid jobid: " + id);
      tracker.killJob(jobid);
      state = new JobState(id, Main.getAppConfigInstance());
      List<JobState> children = state.getChildren();
      if (children != null) {
        for (JobState child : children) {
          try {
            tracker.killJob(StatusDelegator.StringToJobID(child.getId()));
          } catch (IOException e) {
            LOG.warn("templeton: fail to kill job " + child.getId());
          }
        }
      }
      return StatusDelegator.makeStatus(tracker, jobid, state);
    } catch (IllegalStateException e) {
      throw new BadParam(e.getMessage());
    } finally {
      if (tracker != null)
        tracker.close();
      if (state != null)
        state.close();
      if (ugi != null)
        FileSystem.closeAllForUGI(ugi);
    }
  }
}
