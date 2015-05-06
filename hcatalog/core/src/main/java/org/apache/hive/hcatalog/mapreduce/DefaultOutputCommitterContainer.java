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

package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Part of the DefaultOutput*Container classes
 * See {@link DefaultOutputFormatContainer} for more information
 */
class DefaultOutputCommitterContainer extends OutputCommitterContainer {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultOutputCommitterContainer.class);

  /**
   * @param context current JobContext
   * @param baseCommitter OutputCommitter to contain
   * @throws IOException
   */
  public DefaultOutputCommitterContainer(JobContext context, org.apache.hadoop.mapred.OutputCommitter baseCommitter) throws IOException {
    super(context, baseCommitter);
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    getBaseOutputCommitter().abortTask(HCatMapRedUtil.createTaskAttemptContext(context));
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    getBaseOutputCommitter().commitTask(HCatMapRedUtil.createTaskAttemptContext(context));
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    return getBaseOutputCommitter().needsTaskCommit(HCatMapRedUtil.createTaskAttemptContext(context));
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    getBaseOutputCommitter().setupJob(HCatMapRedUtil.createJobContext(context));
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    getBaseOutputCommitter().setupTask(HCatMapRedUtil.createTaskAttemptContext(context));
  }

  @Override
  public void abortJob(JobContext jobContext, State state) throws IOException {
    getBaseOutputCommitter().abortJob(HCatMapRedUtil.createJobContext(jobContext), state);
    cleanupJob(jobContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    getBaseOutputCommitter().commitJob(HCatMapRedUtil.createJobContext(jobContext));
    cleanupJob(jobContext);
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {
    getBaseOutputCommitter().cleanupJob(HCatMapRedUtil.createJobContext(context));

    //Cancel HCat and JobTracker tokens
    IMetaStoreClient client = null;
    try {
      HiveConf hiveConf = HCatUtil.getHiveConf(context.getConfiguration());
      client = HCatUtil.getHiveMetastoreClient(hiveConf);
      String tokenStrForm = client.getTokenStrForm();
      if (tokenStrForm != null && context.getConfiguration().get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
        client.cancelDelegationToken(tokenStrForm);
      }
    } catch (Exception e) {
      LOG.warn("Failed to cancel delegation token", e);
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
  }
}
