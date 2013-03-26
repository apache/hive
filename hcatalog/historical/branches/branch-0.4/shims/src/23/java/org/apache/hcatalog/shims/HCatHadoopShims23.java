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
package org.apache.hcatalog.shims;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.util.Progressable;
import org.apache.pig.ResourceSchema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;


import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.net.NetUtils;

public class HCatHadoopShims23 implements HCatHadoopShims {
    @Override
    public TaskID createTaskID() {
        return new TaskID("", 0, TaskType.MAP, 0);
    }

    @Override
    public TaskAttemptID createTaskAttemptID() {
        return new TaskAttemptID("", 0, TaskType.MAP, 0, 0);
    }

    @Override
    public org.apache.hadoop.mapreduce.TaskAttemptContext createTaskAttemptContext(Configuration conf,
            org.apache.hadoop.mapreduce.TaskAttemptID taskId) {
        return new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(
            conf instanceof JobConf? new JobConf(conf) : conf,
            taskId);
    }

    @Override
    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContext(org.apache.hadoop.mapred.JobConf conf,
            org.apache.hadoop.mapred.TaskAttemptID taskId, Progressable progressable) {
        org.apache.hadoop.mapred.TaskAttemptContext newContext = null;
        try {
            java.lang.reflect.Constructor construct = org.apache.hadoop.mapred.TaskAttemptContextImpl.class.getDeclaredConstructor(
                    org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapred.TaskAttemptID.class,
                    Reporter.class);
            construct.setAccessible(true);
            newContext = (org.apache.hadoop.mapred.TaskAttemptContext)construct.newInstance(
                new JobConf(conf), taskId, (Reporter)progressable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return newContext;
    }

    @Override
    public JobContext createJobContext(Configuration conf,
            JobID jobId) {
        return new JobContextImpl(conf instanceof JobConf? new JobConf(conf) : conf,
                                  jobId);
    }

    @Override
    public org.apache.hadoop.mapred.JobContext createJobContext(org.apache.hadoop.mapred.JobConf conf,
            org.apache.hadoop.mapreduce.JobID jobId, Progressable progressable) {
        return new org.apache.hadoop.mapred.JobContextImpl(
            new JobConf(conf), jobId, (org.apache.hadoop.mapred.Reporter)progressable);
    }

    @Override
    public void commitJob(OutputFormat outputFormat, ResourceSchema schema,
            String arg1, Job job) throws IOException {
        // Do nothing as this was fixed by MAPREDUCE-1447.
    }

    @Override
    public void abortJob(OutputFormat outputFormat, Job job) throws IOException {
        // Do nothing as this was fixed by MAPREDUCE-1447.
    }

    @Override
    public InetSocketAddress getResourceManagerAddress(Configuration conf) {
        String addr = conf.get("yarn.resourcemanager.address", "localhost:8032");

        return NetUtils.createSocketAddr(addr);
    }

    @Override
    public String getPropertyName(PropertyName name) {
        switch (name) {
            case CACHE_ARCHIVES:
                return MRJobConfig.CACHE_ARCHIVES;
            case CACHE_FILES:
                return MRJobConfig.CACHE_FILES;
            case CACHE_SYMLINK:
                return MRJobConfig.CACHE_SYMLINK;
        }

        return "";
    }

    @Override
    public boolean isFileInHDFS(FileSystem fs, Path path) throws IOException {
        // In case of viewfs we need to lookup where the actual file is to know the filesystem in use.
        // resolvePath is a sure shot way of knowing which file system the file is.
        return "hdfs".equals(fs.resolvePath(path).toUri().getScheme());
    }
}
