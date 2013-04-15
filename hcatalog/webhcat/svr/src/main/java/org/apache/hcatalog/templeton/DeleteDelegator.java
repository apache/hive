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
package org.apache.hcatalog.templeton;

import java.io.IOException;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TempletonJobTracker;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hcatalog.templeton.tool.JobState;

/**
 * Delete a job
 */
public class DeleteDelegator extends TempletonDelegator {
    public DeleteDelegator(AppConfig appConf) {
        super(appConf);
    }

    public QueueStatusBean run(String user, String id)
        throws NotAuthorizedException, BadParam, IOException, InterruptedException
    {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        TempletonJobTracker tracker = null;
        JobState state = null;
        try {
            tracker = new TempletonJobTracker(getAddress(appConf),
                                              appConf);
            JobID jobid = StatusDelegator.StringToJobID(id);
            if (jobid == null)
                throw new BadParam("Invalid jobid: " + id);
            tracker.killJob(jobid);
            state = new JobState(id, Main.getAppConfigInstance());
            String childid = state.getChildId();
            if (childid != null)
                tracker.killJob(StatusDelegator.StringToJobID(childid));
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
}
