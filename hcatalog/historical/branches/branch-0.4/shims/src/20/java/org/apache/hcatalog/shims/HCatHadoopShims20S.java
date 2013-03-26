package org.apache.hcatalog.shims;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.util.Progressable;
import org.apache.pig.ResourceSchema;

public class HCatHadoopShims20S implements HCatHadoopShims {
    @Override
    public TaskID createTaskID() {
        return new TaskID();
    }

    @Override
    public TaskAttemptID createTaskAttemptID() {
        return new TaskAttemptID();
    }

	@Override
	public TaskAttemptContext createTaskAttemptContext(Configuration conf,
			TaskAttemptID taskId) {
        return new TaskAttemptContext(conf, taskId);
    }

    @Override
    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContext(org.apache.hadoop.mapred.JobConf conf,
            org.apache.hadoop.mapred.TaskAttemptID taskId, Progressable progressable) {
        org.apache.hadoop.mapred.TaskAttemptContext newContext = null;
        try {
            java.lang.reflect.Constructor construct = org.apache.hadoop.mapred.TaskAttemptContext.class.getDeclaredConstructor(
                    org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapred.TaskAttemptID.class,
                    Progressable.class);
            construct.setAccessible(true);
            newContext = (org.apache.hadoop.mapred.TaskAttemptContext)construct.newInstance(conf, taskId, progressable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return newContext;
    }

    @Override
    public JobContext createJobContext(Configuration conf,
            JobID jobId) {
        return new JobContext(conf, jobId);
    }

    @Override
    public org.apache.hadoop.mapred.JobContext createJobContext(org.apache.hadoop.mapred.JobConf conf,
            org.apache.hadoop.mapreduce.JobID jobId, Progressable progressable) {
        org.apache.hadoop.mapred.JobContext newContext = null;
        try {
            java.lang.reflect.Constructor construct = org.apache.hadoop.mapred.JobContext.class.getDeclaredConstructor(
                    org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapreduce.JobID.class,
                    Progressable.class);
            construct.setAccessible(true);
            newContext = (org.apache.hadoop.mapred.JobContext)construct.newInstance(conf, jobId, progressable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return newContext;
    }

    @Override
    public void commitJob(OutputFormat outputFormat, ResourceSchema schema,
            String arg1, Job job) throws IOException {
        if( job.getConfiguration().get("mapred.job.tracker", "").equalsIgnoreCase("local") ) {
            try {
                //In local mode, mapreduce will not call OutputCommitter.cleanupJob.
                //Calling it from here so that the partition publish happens.
                //This call needs to be removed after MAPREDUCE-1447 is fixed.
                outputFormat.getOutputCommitter(HCatHadoopShims.Instance.get().createTaskAttemptContext(
                            job.getConfiguration(), HCatHadoopShims.Instance.get().createTaskAttemptID())).commitJob(job);
            } catch (IOException e) {
                throw new IOException("Failed to cleanup job",e);
            } catch (InterruptedException e) {
                throw new IOException("Failed to cleanup job",e);
            }
        }
    }

    @Override
    public void abortJob(OutputFormat outputFormat, Job job) throws IOException {
        if (job.getConfiguration().get("mapred.job.tracker", "")
                .equalsIgnoreCase("local")) {
            try {
                // This call needs to be removed after MAPREDUCE-1447 is fixed.
                outputFormat.getOutputCommitter(HCatHadoopShims.Instance.get().createTaskAttemptContext(
                            job.getConfiguration(), new TaskAttemptID())).abortJob(job, State.FAILED);
            } catch (IOException e) {
                throw new IOException("Failed to abort job", e);
            } catch (InterruptedException e) {
                throw new IOException("Failed to abort job", e);
            }
        }
    }

    @Override
    public InetSocketAddress getResourceManagerAddress(Configuration conf)
    {
        return JobTracker.getAddress(conf);
    }

    @Override
    public String getPropertyName(PropertyName name) {
        switch (name) {
            case CACHE_ARCHIVES:
                return DistributedCache.CACHE_ARCHIVES;
            case CACHE_FILES:
                return DistributedCache.CACHE_FILES;
            case CACHE_SYMLINK:
                return DistributedCache.CACHE_SYMLINK;
        }

        return "";
    }

    @Override
    public boolean isFileInHDFS(FileSystem fs, Path path) throws IOException {
        // In hadoop 1.x.x the file system URI is sufficient to determine the uri of the file
        return "hdfs".equals(fs.getUri().getScheme());
    }
}
