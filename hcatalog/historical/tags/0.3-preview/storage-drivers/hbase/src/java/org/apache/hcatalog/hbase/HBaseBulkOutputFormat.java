package org.apache.hcatalog.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Class which imports data into HBase via it's "bulk load" feature. Wherein regions
 * are created by the MR job using HFileOutputFormat and then later "moved" into
 * the appropriate region server.
 */
class HBaseBulkOutputFormat extends OutputFormat<WritableComparable<?>,Put> {
    private final static ImmutableBytesWritable EMPTY_LIST = new ImmutableBytesWritable(new byte[0]);
    private SequenceFileOutputFormat<WritableComparable<?>,Put> baseOutputFormat;

    public HBaseBulkOutputFormat() {
        baseOutputFormat = new SequenceFileOutputFormat<WritableComparable<?>,Put>();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        baseOutputFormat.checkOutputSpecs(context);
        //Get jobTracker delegation token if security is enabled
        //we need to launch the ImportSequenceFile job
        if(context.getConfiguration().getBoolean("hadoop.security.authorization",false)) {
            JobClient jobClient = new JobClient(new JobConf(context.getConfiguration()));
            context.getCredentials().addToken(new Text("my mr token"), jobClient.getDelegationToken(null));
        }
    }

    @Override
    public RecordWriter<WritableComparable<?>, Put> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        //TODO use a constant/static setter when available
        context.getConfiguration().setClass("mapred.output.key.class",ImmutableBytesWritable.class,Object.class);
        context.getConfiguration().setClass("mapred.output.value.class",Put.class,Object.class);
        return new HBaseBulkRecordWriter(baseOutputFormat.getRecordWriter(context));
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        return new HBaseBulkOutputCommitter(baseOutputFormat.getOutputCommitter(context));
    }

    private static class HBaseBulkRecordWriter extends  RecordWriter<WritableComparable<?>,Put> {
        private RecordWriter<WritableComparable<?>,Put> baseWriter;

        public HBaseBulkRecordWriter(RecordWriter<WritableComparable<?>,Put> baseWriter)  {
            this.baseWriter = baseWriter;
        }

        @Override
        public void write(WritableComparable<?> key, Put value) throws IOException, InterruptedException {
            //we ignore the key
            baseWriter.write(EMPTY_LIST, value);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            baseWriter.close(context);
        }
    }

    private static class HBaseBulkOutputCommitter extends OutputCommitter {
        private OutputCommitter baseOutputCommitter;

        public HBaseBulkOutputCommitter(OutputCommitter baseOutputCommitter) throws IOException {
            this.baseOutputCommitter = baseOutputCommitter;
        }

        @Override
        public void abortTask(TaskAttemptContext context) throws IOException {
            baseOutputCommitter.abortTask(context);
        }

        @Override
        public void commitTask(TaskAttemptContext context) throws IOException {
            baseOutputCommitter.commitTask(context);
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
            return baseOutputCommitter.needsTaskCommit(context);
        }

        @Override
        public void setupJob(JobContext context) throws IOException {
            baseOutputCommitter.setupJob(context);
        }

        @Override
        public void setupTask(TaskAttemptContext context) throws IOException {
            baseOutputCommitter.setupTask(context);
        }

        @Override
        public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
            try {
                baseOutputCommitter.abortJob(jobContext,state);
            } finally {
                cleanIntermediate(jobContext);
            }
        }

        @Override
        public void cleanupJob(JobContext context) throws IOException {
            try {
                baseOutputCommitter.cleanupJob(context);
            } finally {
                cleanIntermediate(context);
            }
        }

        @Override
        public void commitJob(JobContext jobContext) throws IOException {
            try {
                baseOutputCommitter.commitJob(jobContext);
                Configuration conf = jobContext.getConfiguration();
                Path srcPath = FileOutputFormat.getOutputPath(jobContext);
                Path destPath = new Path(srcPath.getParent(),srcPath.getName()+"_hfiles");
                ImportSequenceFile.runJob(jobContext,
                                                        conf.get(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY),
                                                        srcPath,
                                                        destPath);
            } finally {
                cleanIntermediate(jobContext);
            }
        }

        public void cleanIntermediate(JobContext jobContext) throws IOException {
            FileSystem fs = FileSystem.get(jobContext.getConfiguration());
            fs.delete(FileOutputFormat.getOutputPath(jobContext),true);
        }
    }
}
