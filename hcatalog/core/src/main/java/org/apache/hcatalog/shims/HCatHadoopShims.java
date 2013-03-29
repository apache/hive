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
package org.apache.hcatalog.shims;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.util.Progressable;

/**
 * Shim layer to abstract differences between Hadoop 0.20 and 0.23
 * (HCATALOG-179). This mirrors Hive shims, but is kept separate for HCatalog
 * dependencies.
 **/
public interface HCatHadoopShims {

    enum PropertyName {CACHE_ARCHIVES, CACHE_FILES, CACHE_SYMLINK}

    ;

    public static abstract class Instance {
        static HCatHadoopShims instance = selectShim();

        public static HCatHadoopShims get() {
            return instance;
        }

        private static HCatHadoopShims selectShim() {
            // piggyback on Hive's detection logic
            String major = ShimLoader.getMajorVersion();
            String shimFQN = "org.apache.hcatalog.shims.HCatHadoopShims20S";
            if (major.startsWith("0.23")) {
                shimFQN = "org.apache.hcatalog.shims.HCatHadoopShims23";
            }
            try {
                Class<? extends HCatHadoopShims> clasz = Class.forName(shimFQN)
                    .asSubclass(HCatHadoopShims.class);
                return clasz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate: " + shimFQN, e);
            }
        }
    }

    public TaskID createTaskID();

    public TaskAttemptID createTaskAttemptID();

    public org.apache.hadoop.mapreduce.TaskAttemptContext createTaskAttemptContext(Configuration conf,
                                                                                   TaskAttemptID taskId);

    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContext(JobConf conf,
                                                                                org.apache.hadoop.mapred.TaskAttemptID taskId, Progressable progressable);

    public JobContext createJobContext(Configuration conf, JobID jobId);

    public org.apache.hadoop.mapred.JobContext createJobContext(JobConf conf, JobID jobId, Progressable progressable);

    public void commitJob(OutputFormat outputFormat, Job job) throws IOException;

    public void abortJob(OutputFormat outputFormat, Job job) throws IOException;

    /* Referring to job tracker in 0.20 and resource manager in 0.23 */
    public InetSocketAddress getResourceManagerAddress(Configuration conf);

    public String getPropertyName(PropertyName name);

    /**
     * Checks if file is in HDFS filesystem.
     *
     * @param fs
     * @param path
     * @return true if the file is in HDFS, false if the file is in other file systems.
     */
    public boolean isFileInHDFS(FileSystem fs, Path path) throws IOException;

}
