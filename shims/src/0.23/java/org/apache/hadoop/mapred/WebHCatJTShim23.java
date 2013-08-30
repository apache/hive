package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.HadoopShims.WebHCatJTShim;

import java.io.IOException;

public class WebHCatJTShim23 implements WebHCatJTShim {
  private JobClient jc;

  /**
   * Create a connection to the Job Tracker.
   */
  public WebHCatJTShim23(Configuration conf)
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
