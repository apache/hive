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
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.shims.HadoopShims.WebHCatJTShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * List jobs owned by a user.
 */
public class ListDelegator extends TempletonDelegator {
  public ListDelegator(AppConfig appConf) {
    super(appConf);
  }

  public List<String> run(String user, boolean showall)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException {

    UserGroupInformation ugi = UgiFactory.getUgi(user);
    WebHCatJTShim tracker = null;
    try {
      tracker = ShimLoader.getHadoopShims().getWebHCatShim(appConf, ugi);

      ArrayList<String> ids = new ArrayList<String>();

      JobStatus[] jobs = tracker.getAllJobs();

      if (jobs != null) {
        for (JobStatus job : jobs) {
          String id = job.getJobID().toString();
          if (showall || user.equals(job.getUsername()))
            ids.add(id);
        }
      }

      return ids;
    } catch (IllegalStateException e) {
      throw new BadParam(e.getMessage());
    } finally {
      if (tracker != null)
        tracker.close();
    }
  }
}
