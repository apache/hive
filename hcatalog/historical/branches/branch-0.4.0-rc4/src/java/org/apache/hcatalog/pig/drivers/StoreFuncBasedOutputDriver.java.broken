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

package org.apache.hcatalog.pig.drivers;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.FileOutputStorageDriver;
import org.apache.hcatalog.mapreduce.HCatOutputStorageDriver;
import org.apache.hcatalog.mapreduce.OutputJobInfo;
import org.apache.hcatalog.pig.HCatLoader;
import org.apache.hcatalog.pig.HCatStorer;
import org.apache.hcatalog.pig.PigHCatUtil;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class StoreFuncBasedOutputDriver extends FileOutputStorageDriver {

    protected StoreFuncInterface sf;
    private TupleFactory factory = TupleFactory.getInstance();
    private HCatSchema schema;
    private String location;
    
    @Override
    public void initialize(JobContext jobContext, Properties hcatProperties) throws IOException {
        String storerString = hcatProperties.getProperty(HCatConstants.HCAT_PIG_STORER);
        if (storerString==null) {
            throw new HCatException(ErrorType.ERROR_INIT_STORER, "Don't know how to instantiate storer, " + HCatConstants.HCAT_PIG_STORER + " property is not defined for table ");
        }
        String storerArgs = hcatProperties.getProperty(HCatConstants.HCAT_PIG_STORER_ARGS);
        
        String[] args;
        if (storerArgs!=null) {
            String delimit = hcatProperties.getProperty(HCatConstants.HCAT_PIG_ARGS_DELIMIT);
            if (delimit==null) {
                delimit = HCatConstants.HCAT_PIG_ARGS_DELIMIT_DEFAULT;
            }
            args = storerArgs.split(delimit);
        } else {
            args = new String[0];
        }
        
        try {
            Class storerClass = Class.forName(storerString);
        
            Constructor[] constructors = storerClass.getConstructors();
            for (Constructor constructor : constructors) {
                if (constructor.getParameterTypes().length==args.length) {
                    sf = (StoreFuncInterface)constructor.newInstance(args);
                    break;
                }
            }
        } catch (Exception e) {
            throw new HCatException(ErrorType.ERROR_INIT_STORER, "Cannot instantiate " + storerString, e);
        }
        
        if (sf==null) {
            throw new HCatException(ErrorType.ERROR_INIT_STORER, "Cannot instantiate " + storerString + " with construct args " + storerArgs);
        }
     
        super.initialize(jobContext, hcatProperties);
        
        Job job = new Job(jobContext.getConfiguration());
        String innerSignature = jobContext.getConfiguration().get(HCatStorer.INNER_SIGNATURE);
        
        // Set signature before invoking StoreFunc methods, see comment in
        // see comments in LoadFuncBasedInputDriver.initialize
        sf.setStoreFuncUDFContextSignature(innerSignature);
        sf.checkSchema(PigHCatUtil.getResourceSchema(schema));

        sf.setStoreLocation(location, job);
        ConfigurationUtil.mergeConf(jobContext.getConfiguration(), 
                job.getConfiguration());
    }
    
    @Override
    public OutputFormat<? extends WritableComparable<?>, ? extends Writable> getOutputFormat()
            throws IOException {
        StoreFuncBasedOutputFormat outputFormat = new StoreFuncBasedOutputFormat(sf);
        return outputFormat;
    }

    @Override
    public void setOutputPath(JobContext jobContext, String location)
            throws IOException {
        this.location = location;
    }

    @Override
    public void setSchema(JobContext jobContext, HCatSchema schema)
            throws IOException {
        this.schema = schema;
    }

    @Override
    public void setPartitionValues(JobContext jobContext,
            Map<String, String> partitionValues) throws IOException {
        // Doing nothing, partition keys are not stored along with the data, so ignore it
    }

    @Override
    public WritableComparable<?> generateKey(HCatRecord value)
            throws IOException {
        return null;
    }

    @Override
    public Writable convertValue(HCatRecord value) throws IOException {
        Tuple t = factory.newTupleNoCopy(value.getAll());
        return t;
    }

}
