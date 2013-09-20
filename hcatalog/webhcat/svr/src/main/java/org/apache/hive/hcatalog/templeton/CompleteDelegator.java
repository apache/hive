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
import java.net.URL;
import java.net.MalformedURLException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.hcatalog.templeton.tool.JobState;
import org.apache.hive.hcatalog.templeton.tool.TempletonUtils;

/**
 * Complete a job.  This will run the callback if
 *
 * - the job is done
 * - there is a callback
 * - the callback has not yet been called
 *
 * There is a small chance for a race condition if two callers run
 * this at the same time.  That should never happen.
 *
 * We use a Hadoop config var to notify this class on the completion
 * of a job.  Hadoop will call use multiple times in the event of
 * failure.  Even if the failure is that the client callback failed.
 *
 * See LauncherDelegator for the HADOOP_END_RETRY* vars that are set.
 */
public class CompleteDelegator extends TempletonDelegator {
  private static final Log LOG = LogFactory.getLog(CompleteDelegator.class);

  public CompleteDelegator(AppConfig appConf) {
    super(appConf);
  }

  public CompleteBean run(String id)
    throws CallbackFailedException, IOException {
    if (id == null)
      acceptWithError("No jobid given");

    JobState state = null;
    try {
      state = new JobState(id, Main.getAppConfigInstance());
      if (state.getCompleteStatus() == null)
        failed("Job not yet complete. jobId=" + id, null);

      Long notified = state.getNotifiedTime();
      if (notified != null)
        return acceptWithError("Callback already run for jobId=" + id +
                " at " + new Date(notified));

      String callback = state.getCallback();
      if (callback == null)
        return new CompleteBean("No callback registered");

      try {
        doCallback(state.getId(), callback);
      } catch (Exception e) {
        failed("Callback failed " + callback + " for " + id, e);
      }

      state.setNotifiedTime(System.currentTimeMillis());
      return new CompleteBean("Callback sent");
    } finally {
      if (state != null)
        state.close();
    }
  }

  /**
   * Call the callback url with the jobid to let them know it's
   * finished.  If the url has the string $jobId in it, it will be
   * replaced with the completed jobid.
   */
  public static void doCallback(String jobid, String url)
    throws MalformedURLException, IOException {
    if (url.contains("$jobId"))
      url = url.replace("$jobId", jobid);
    TempletonUtils.fetchUrl(new URL(url));
  }

  private void failed(String msg, Exception e)
    throws CallbackFailedException {
    if (e != null)
      LOG.error(msg, e);
    else
      LOG.error(msg);
    throw new CallbackFailedException(msg);
  }

  private CompleteBean acceptWithError(String msg) {
    LOG.error(msg);
    return new CompleteBean(msg);
  }
}
