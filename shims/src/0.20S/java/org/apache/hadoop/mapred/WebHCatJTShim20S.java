package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.HadoopShims.WebHCatJTShim;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This is in org.apache.hadoop.mapred package because it relies on 
 * JobSubmissionProtocol which is package private
 */
public class WebHCatJTShim20S implements WebHCatJTShim {
    private JobSubmissionProtocol cnx;

    /**
     * Create a connection to the Job Tracker.
     */
    public WebHCatJTShim20S(Configuration conf)
            throws IOException {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      cnx = (JobSubmissionProtocol)
              RPC.getProxy(JobSubmissionProtocol.class,
                      JobSubmissionProtocol.versionID,
                      getAddress(conf),
                      ugi,
                      conf,
                      NetUtils.getSocketFactory(conf,
                              JobSubmissionProtocol.class));
    }

    /**
     * Grab a handle to a job that is already known to the JobTracker.
     *
     * @return Profile of the job, or null if not found.
     */
    public JobProfile getJobProfile(org.apache.hadoop.mapred.JobID jobid)
            throws IOException {
      return cnx.getJobProfile(jobid);
    }

    /**
     * Grab a handle to a job that is already known to the JobTracker.
     *
     * @return Status of the job, or null if not found.
     */
    public org.apache.hadoop.mapred.JobStatus getJobStatus(org.apache.hadoop.mapred.JobID jobid)
            throws IOException {
      return cnx.getJobStatus(jobid);
    }


    /**
     * Kill a job.
     */
    public void killJob(org.apache.hadoop.mapred.JobID jobid)
            throws IOException {
      cnx.killJob(jobid);
    }

    /**
     * Get all the jobs submitted.
     */
    public org.apache.hadoop.mapred.JobStatus[] getAllJobs()
            throws IOException {
      return cnx.getAllJobs();
    }

    /**
     * Close the connection to the Job Tracker.
     */
    public void close() {
      RPC.stopProxy(cnx);
    }
    private InetSocketAddress getAddress(Configuration conf) {
      String jobTrackerStr = conf.get("mapred.job.tracker", "localhost:8012");
      return NetUtils.createSocketAddr(jobTrackerStr);
    }
  }

