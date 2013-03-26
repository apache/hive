/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;


/** The abstract class to be implemented by underlying storage drivers to enable data access from HCat through
 *  HCatOutputFormat.
 */
public abstract class HCatOutputStorageDriver {


  /**
   * Initialize the storage driver with specified properties, default implementation does nothing.
   * @param context the job context object
   * @param hcatProperties the properties for the storage driver
   * @throws IOException Signals that an I/O exception has occurred.
   */
    public void initialize(JobContext context, Properties hcatProperties) throws IOException {
    }

    /**
     * Returns the OutputFormat to use with this Storage Driver.
     * @return the OutputFormat instance
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract OutputFormat<? extends WritableComparable<?>, ? extends Writable> getOutputFormat() throws IOException;

    /**
     * Set the data location for the output.
     * @param jobContext the job context object
     * @param location the data location
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void setOutputPath(JobContext jobContext, String location) throws IOException;

    /**
     * Set the schema for the data being written out.
     * @param jobContext the job context object
     * @param schema the data schema
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void setSchema(JobContext jobContext, HCatSchema schema) throws IOException;

    /**
     * Sets the partition key values for the partition being written.
     * @param jobContext the job context object
     * @param partitionValues the partition values
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues) throws IOException;

    /**
     * Generate the key for the underlying outputformat. The value given to HCatOutputFormat is passed as the
     * argument. The key given to HCatOutputFormat is ignored..
     * @param value the value given to HCatOutputFormat
     * @return a key instance
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract WritableComparable<?> generateKey(HCatRecord value) throws IOException;

    /**
     * Convert the given HCatRecord value to the actual value type.
     * @param value the HCatRecord value to convert
     * @return a value instance
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract Writable convertValue(HCatRecord value) throws IOException;

    /**
     * Gets the location to use for the specified partition values.
     *  The storage driver can override as required.
     * @param jobContext the job context object
     * @param tableLocation the location of the table
     * @param partitionValues the partition values
     * @param dynHash A unique hash value that represents the dynamic partitioning job used
     * @return the location String.
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public String getOutputLocation(JobContext jobContext,
            String tableLocation, List<String> partitionCols, Map<String, String> partitionValues, String dynHash) throws IOException {
      return null;
    }

    /** Storage drivers wrapping other OutputFormats should override this method.
     */
    public Path getWorkFilePath(TaskAttemptContext context, String outputLoc) throws IOException{
      return null;
    }

    /**
     * Implementation that calls the underlying output committer's setupJob, 
     * used in lieu of underlying committer's setupJob when using dynamic partitioning
     * The default implementation should be overriden by underlying implementations
     * that do not use FileOutputCommitter.
     * The reason this function exists is so as to allow a storage driver implementor to
     * override underlying OutputCommitter's setupJob implementation to allow for
     * being called multiple times in a job, to make it idempotent.
     * This should be written in a manner that is callable multiple times 
     * from individual tasks without stepping on each others' toes
     * 
     * @param context
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void setupOutputCommitterJob(TaskAttemptContext context) 
        throws IOException, InterruptedException{
      getOutputFormat().getOutputCommitter(context).setupJob(context);
    }

    /**
     * Implementation that calls the underlying output committer's cleanupJob, 
     * used in lieu of underlying committer's cleanupJob when using dynamic partitioning
     * This should be written in a manner that is okay to call after having had
     * multiple underlying outputcommitters write to task dirs inside it.
     * While the base MR cleanupJob should have sufficed normally, this is provided
     * in order to let people implementing setupOutputCommitterJob to cleanup properly
     * 
     * @param context
     * @throws IOException 
     */
    public void cleanupOutputCommitterJob(TaskAttemptContext context) 
        throws IOException, InterruptedException{
      getOutputFormat().getOutputCommitter(context).cleanupJob(context);
    }

    /**
     * Implementation that calls the underlying output committer's abortJob, 
     * used in lieu of underlying committer's abortJob when using dynamic partitioning
     * This should be written in a manner that is okay to call after having had
     * multiple underlying outputcommitters write to task dirs inside it.
     * While the base MR cleanupJob should have sufficed normally, this is provided
     * in order to let people implementing setupOutputCommitterJob to abort properly
     * 
     * @param context
     * @param state
     * @throws IOException 
     */
    public void abortOutputCommitterJob(TaskAttemptContext context, State state) 
        throws IOException, InterruptedException{
      getOutputFormat().getOutputCommitter(context).abortJob(context,state);
    }

    /**
     * return an instance of OutputFormatContainer containing the passed outputFormat. DefaultOutputFormatContainer is returned by default.
     * @param outputFormat format the returned container will contain
     * @return
     */
    OutputFormatContainer getOutputFormatContainer(OutputFormat outputFormat) {
        return new DefaultOutputFormatContainer(outputFormat);
    }

}
