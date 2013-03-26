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

package org.apache.hcatalog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hcatalog.common.HCatConstants;

/**
 * Part of the DefaultOutput*Container classes
 * See {@link DefaultOutputFormatContainer} for more information
 */
class DefaultOutputCommitterContainer extends OutputCommitterContainer {

    /**
     * @param context current JobContext
     * @param baseCommitter OutputCommitter to contain
     * @throws IOException
     */
    public DefaultOutputCommitterContainer(JobContext context, OutputCommitter baseCommitter) throws IOException {
        super(context,baseCommitter);
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
        getBaseOutputCommitter().abortTask(context);
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        getBaseOutputCommitter().commitTask(context);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return getBaseOutputCommitter().needsTaskCommit(context);
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        getBaseOutputCommitter().setupJob(context);
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        getBaseOutputCommitter().setupTask(context);
    }

    @Override
    public void abortJob(JobContext jobContext, State state) throws IOException {
        getBaseOutputCommitter().abortJob(jobContext, state);
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
        getBaseOutputCommitter().commitJob(jobContext);
    }

    @Override
    public void cleanupJob(JobContext context) throws IOException {
        getBaseOutputCommitter().cleanupJob(context);

        OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context);

        //Cancel HCat and JobTracker tokens
        try {
            HiveMetaStoreClient client = HCatOutputFormat.createHiveClient(jobInfo.getServerUri(), context.getConfiguration());
            String tokenStrForm = client.getTokenStrForm();
            if(tokenStrForm != null && context.getConfiguration().get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
              client.cancelDelegationToken(tokenStrForm);
            }
        } catch (Exception e) {
            throw new IOException("Failed to cancel delegation token",e);
        }
    }
}
