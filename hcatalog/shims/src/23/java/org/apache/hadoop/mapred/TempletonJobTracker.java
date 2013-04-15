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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;

/*
 * Communicate with the JobTracker as a specific user.
 */
public class TempletonJobTracker {
    private JobClient jc;

    /**
     * Create a connection to the Job Tracker.
     */
    public TempletonJobTracker(InetSocketAddress addr,
                               Configuration conf)
        throws IOException {
        
        jc = new JobClient(conf);
    }

    /**
     * Grab a handle to a job that is already known to the JobTracker.
     *
     * @return Profile of the job, or null if not found.
     */
    public JobProfile getJobProfile(JobID jobid)
        throws IOException {
        RunningJob rj = jc.getJob(jobid);
        JobStatus jobStatus = rj.getJobStatus();
        JobProfile jobProfile = new JobProfile(jobStatus.getUsername(), jobStatus.getJobID(), 
                jobStatus.getJobFile(), jobStatus.getTrackingUrl(), jobStatus.getJobName());
        return jobProfile;
    }

    /**
     * Grab a handle to a job that is already known to the JobTracker.
     *
     * @return Status of the job, or null if not found.
     */
    public JobStatus getJobStatus(JobID jobid)
        throws IOException {
        RunningJob rj = jc.getJob(jobid);
        JobStatus jobStatus = rj.getJobStatus();
        return jobStatus;
    }


    /**
     * Kill a job.
     */
    public void killJob(JobID jobid)
        throws IOException {
        RunningJob rj = jc.getJob(jobid);
        rj.killJob();
    }

    /**
     * Get all the jobs submitted.
     */
    public JobStatus[] getAllJobs()
        throws IOException {
        return jc.getAllJobs();
    }

    /**
     * Close the connection to the Job Tracker.
     */
    public void close() {
        try {
            jc.close();
        } catch (IOException e) {
        }
    }
}
