package org.apache.hcatalog.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatOutputStorageDriver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Forwarding HBaseOutputStorageDriver, actual implementation is decided by a configuration
 * {@link HBaseConstants.PROPERTY_OSD_BULK_MODE_KEY} which defaults to HBaseBulkOutputStorageDriver
 */
public class HBaseOutputStorageDriver extends HCatOutputStorageDriver {

    private HBaseBulkOutputStorageDriver bulkOSD = new HBaseBulkOutputStorageDriver();
    private HBaseDirectOutputStorageDriver directOSD = new HBaseDirectOutputStorageDriver();
    private HBaseBaseOutputStorageDriver activeOSD;

    @Override
    public void initialize(JobContext context, Properties hcatProperties) throws IOException {
        super.initialize(context, hcatProperties);
        determineOSD(context.getConfiguration(),hcatProperties);
        activeOSD.initialize(context,hcatProperties);
    }

    @Override
    public WritableComparable<?> generateKey(HCatRecord value) throws IOException {
        return activeOSD.generateKey(value);
    }

    @Override
    public Writable convertValue(HCatRecord value) throws IOException {
        return activeOSD.convertValue(value);
    }

    @Override
    public String getOutputLocation(JobContext jobContext, String tableLocation, List<String> partitionCols, Map<String, String> partitionValues, String dynHash) throws IOException {
        //sanity check since we can't determine which will be used till initialize
        //and this method gets called before that
        String l1 = bulkOSD.getOutputLocation(jobContext, tableLocation, partitionCols, partitionValues, dynHash);
        String l2 = directOSD.getOutputLocation(jobContext, tableLocation, partitionCols, partitionValues, dynHash);
        if(l1 != null || l2 != null) {
            throw new IOException("bulkOSD or directOSD returns a non-null path for getOutputLocation()");
        }
        return null;
    }

    @Override
    public Path getWorkFilePath(TaskAttemptContext context, String outputLoc) throws IOException {
        return activeOSD.getWorkFilePath(context,outputLoc);
    }

    @Override
    public OutputFormat<? extends WritableComparable<?>, ? extends Writable> getOutputFormat() throws IOException {
        return activeOSD.getOutputFormat();
    }

    @Override
    public void setOutputPath(JobContext jobContext, String location) throws IOException {
        directOSD.setOutputPath(jobContext, location);
        bulkOSD.setOutputPath(jobContext, location);
    }

    @Override
    public void setSchema(JobContext jobContext, HCatSchema schema) throws IOException {
        directOSD.setSchema(jobContext,schema);
        bulkOSD.setSchema(jobContext,schema);
    }

    @Override
    public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues) throws IOException {
        directOSD.setPartitionValues(jobContext,partitionValues);
        bulkOSD.setPartitionValues(jobContext,partitionValues);
    }

    private void determineOSD(Configuration conf, Properties prop) {
        if(activeOSD != null)
            return;

        String bulkMode = conf.get(HBaseConstants.PROPERTY_OSD_BULK_MODE_KEY);
        if(bulkMode == null && prop != null)
            bulkMode = prop.getProperty(HBaseConstants.PROPERTY_OSD_BULK_MODE_KEY);

        if(bulkMode != null && !Boolean.valueOf(bulkMode)) {
            activeOSD = directOSD;
            bulkOSD = null;
        }
        else {
            activeOSD = bulkOSD;
            directOSD = null;
        }
    }
}
