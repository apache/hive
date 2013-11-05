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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.shims.HadoopShims.WebHCatJTShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobProfile;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hive.hcatalog.templeton.tool.JobState;

/**
 * Fetch the status of a given job id in the queue. There are three sources of the info
 * 1. Query result from JobTracker
 * 2. JobState saved by TempletonControllerJob when monitoring the TempletonControllerJob
 * 3. TempletonControllerJob put a JobState for every job it launches, so child job can
 *    retrieve its parent job by its JobState
 * 
 * Currently there is no permission restriction, any user can query any job
 */
public class StatusDelegator extends TempletonDelegator {
  private static final Log LOG = LogFactory.getLog(StatusDelegator.class);

  public StatusDelegator(AppConfig appConf) {
    super(appConf);
  }

  public QueueStatusBean run(String user, String id)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException
  {
    WebHCatJTShim tracker = null;
    JobState state = null;
    try {
      UserGroupInformation ugi = UgiFactory.getUgi(user);
      tracker = ShimLoader.getHadoopShims().getWebHCatShim(appConf, ugi);
      JobID jobid = StatusDelegator.StringToJobID(id);
      if (jobid == null)
        throw new BadParam("Invalid jobid: " + id);
      state = new JobState(id, Main.getAppConfigInstance());
      return StatusDelegator.makeStatus(tracker, jobid, state);
    } catch (IllegalStateException e) {
      throw new BadParam(e.getMessage());
    } finally {
      if (tracker != null)
        tracker.close();
      if (state != null)
        state.close();
    }
  }

  static QueueStatusBean makeStatus(WebHCatJTShim tracker,
                       JobID jobid,
                       JobState state)
    throws BadParam, IOException {

    JobStatus status = tracker.getJobStatus(jobid);
    JobProfile profile = tracker.getJobProfile(jobid);
    if (status == null || profile == null) // No such job.
      throw new BadParam("Could not find job " + jobid);

    return new QueueStatusBean(state, status, profile);
  }

  /**
   * A version of JobID.forName with our app specific error handling.
   */
  public static JobID StringToJobID(String id)
    throws BadParam {
    try {
      return JobID.forName(id);
    } catch (IllegalArgumentException e) {
      throw new BadParam(e.getMessage());
    }
  }
}
