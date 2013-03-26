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

package org.apache.hcatalog.hbase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Storage driver which works with {@link HBaseBulkOutputFormat} and makes use
 * of HBase's "bulk load" feature to get data into HBase. This should be
 * efficient for large batch writes in comparison to HBaseDirectOutputStorageDriver.
 */
public class HBaseBulkOutputStorageDriver extends HBaseBaseOutputStorageDriver {
    private String PROPERTY_TABLE_LOCATION = "hcat.hbase.mapreduce.table.location";
    private String PROPERTY_INT_OUTPUT_LOCATION = "hcat.hbase.mapreduce.intermediateOutputLocation";
    private OutputFormat outputFormat;
    private final static ImmutableBytesWritable EMPTY_KEY = new ImmutableBytesWritable(new byte[0]);

    @Override
    public void initialize(JobContext context, Properties hcatProperties) throws IOException {
        super.initialize(context, hcatProperties);

        //initialize() gets called multiple time in the lifecycle of an MR job, client, mapper, reducer, etc
        //depending on the case we have to make sure for some context variables we set here that they don't get set again
        if(!outputJobInfo.getProperties().containsKey(PROPERTY_INT_OUTPUT_LOCATION)) {
            String tableLocation = context.getConfiguration().get(PROPERTY_TABLE_LOCATION);
            String location = new  Path(tableLocation,
                                                    "REVISION_"+outputJobInfo.getProperties()
                                                                                               .getProperty(HBaseConstants.PROPERTY_OUTPUT_VERSION_KEY)).toString();
            outputJobInfo.getProperties().setProperty(PROPERTY_INT_OUTPUT_LOCATION, location);
            //We are writing out an intermediate sequenceFile hence location is not passed in OutputJobInfo.getLocation()
            //TODO replace this with a mapreduce constant when available
            context.getConfiguration().set("mapred.output.dir", location);
            //Temporary fix until support for secure hbase is available
            //We need the intermediate directory to be world readable
            //so that the hbase user can import the generated hfiles
            if(context.getConfiguration().getBoolean("hadoop.security.authorization",false)) {
                Path p = new Path(tableLocation);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                fs.setPermission(new Path(tableLocation),
                                        FsPermission.valueOf("drwx--x--x"));
                while((p = p.getParent()) != null) {
                    if(!fs.getFileStatus(p).getPermission().getOtherAction().implies(FsAction.EXECUTE))
                        throw new IOException("Table's parent directories must at least have global execute permissions.");
                }
            }
        }

        outputFormat = new HBaseBulkOutputFormat();
        context.getConfiguration().set(HCatConstants.HCAT_KEY_OUTPUT_INFO, HCatUtil.serialize(outputJobInfo));
    }

    @Override
    public OutputFormat<? extends WritableComparable<?>, ? extends Writable> getOutputFormat() throws IOException {
        return outputFormat;
    }

    @Override
    public WritableComparable<?> generateKey(HCatRecord value) throws IOException {
        return EMPTY_KEY;
    }

    @Override
    public String getOutputLocation(JobContext jobContext, String tableLocation, List<String> partitionCols, Map<String, String> partitionValues, String dynHash) throws IOException {
        //TODO have HCatalog common objects expose more information
        //this is the only way to pickup table location for storageDrivers
        jobContext.getConfiguration().set(PROPERTY_TABLE_LOCATION, tableLocation);
        return null;
    }
}
