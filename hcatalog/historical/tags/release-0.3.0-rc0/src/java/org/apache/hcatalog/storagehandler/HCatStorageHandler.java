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

package org.apache.hcatalog.storagehandler;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.mapreduce.HCatInputStorageDriver;
import org.apache.hcatalog.mapreduce.HCatOutputStorageDriver;

/**
 * The abstract Class HCatStorageHandler would server as the base class for all
 * the storage handlers required for non-native tables in HCatalog.
 */
public abstract class HCatStorageHandler implements HiveMetaHook,
        HiveStorageHandler {
    
    /**
     * Gets the input storage driver.
     * 
     * @return the input storage driver
     */
    public abstract Class<? extends HCatInputStorageDriver> getInputStorageDriver();
    
    /**
     * Gets the output storage driver.
     * 
     * @return the output storage driver
     */
    public abstract Class<? extends HCatOutputStorageDriver> getOutputStorageDriver();
    
    /**
     * 
     * 
     * @return authorization provider
     * @throws HiveException
     */
    public abstract HiveAuthorizationProvider getAuthorizationProvider()
            throws HiveException;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.metastore.HiveMetaHook#commitCreateTable(org.apache
     * .hadoop.hive.metastore.api.Table)
     */
    @Override
    public abstract void commitCreateTable(Table table) throws MetaException;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.metastore.HiveMetaHook#commitDropTable(org.apache
     * .hadoop.hive.metastore.api.Table, boolean)
     */
    @Override
    public abstract void commitDropTable(Table table, boolean deleteData)
            throws MetaException;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.metastore.HiveMetaHook#preCreateTable(org.apache
     * .hadoop.hive.metastore.api.Table)
     */
    @Override
    public abstract void preCreateTable(Table table) throws MetaException;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.metastore.HiveMetaHook#preDropTable(org.apache
     * .hadoop.hive.metastore.api.Table)
     */
    @Override
    public abstract void preDropTable(Table table) throws MetaException;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.metastore.HiveMetaHook#rollbackCreateTable(org
     * .apache.hadoop.hive.metastore.api.Table)
     */
    @Override
    public abstract void rollbackCreateTable(Table table) throws MetaException;
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.hive.metastore.HiveMetaHook#rollbackDropTable
     * (org.apache.hadoop.hive.metastore.api.Table)
     */
    @Override
    public abstract void rollbackDropTable(Table table) throws MetaException;
    
    @Override
    public abstract HiveMetaHook getMetaHook();
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.hive.ql.metadata.HiveStorageHandler#
     * configureTableJobProperties(org.apache.hadoop.hive.ql.plan.TableDesc,
     * java.util.Map)
     */
    @Override
    public abstract void configureTableJobProperties(TableDesc tableDesc,
            Map<String, String> jobProperties);
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    @Override
    public abstract Configuration getConf();
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.
     * Configuration)
     */
    @Override
    public abstract void setConf(Configuration conf);
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.ql.metadata.HiveStorageHandler#getSerDeClass()
     */
    @Override
    public abstract Class<? extends SerDe> getSerDeClass();
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.ql.metadata.HiveStorageHandler#getInputFormatClass
     * ()
     */
    @Override
    public final Class<? extends InputFormat> getInputFormatClass() {
        return DummyInputFormat.class;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.ql.metadata.HiveStorageHandler#getOutputFormatClass
     * ()
     */
    @Override
    public final Class<? extends OutputFormat> getOutputFormatClass() {
        return DummyOutputFormat.class;
    }
    
    /**
     * The Class DummyInputFormat is a dummy implementation of the old hadoop
     * mapred.InputFormat required by HiveStorageHandler.
     */
    class DummyInputFormat implements
            InputFormat<WritableComparable, HCatRecord> {
        
        /*
         * @see
         * org.apache.hadoop.mapred.InputFormat#getRecordReader(org.apache.hadoop
         * .mapred.InputSplit, org.apache.hadoop.mapred.JobConf,
         * org.apache.hadoop.mapred.Reporter)
         */
        @Override
        public RecordReader<WritableComparable, HCatRecord> getRecordReader(
                InputSplit split, JobConf jobconf, Reporter reporter)
                throws IOException {
            throw new IOException("This operation is not supported.");
        }
        
        /*
         * @see
         * org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.
         * mapred .JobConf, int)
         */
        @Override
        public InputSplit[] getSplits(JobConf jobconf, int number)
                throws IOException {
            throw new IOException("This operation is not supported.");
        }
    }
    
    /**
     * The Class DummyOutputFormat is a dummy implementation of the old hadoop
     * mapred.OutputFormat and HiveOutputFormat required by HiveStorageHandler.
     */
    class DummyOutputFormat implements
            OutputFormat<WritableComparable<?>, HCatRecord>,
            HiveOutputFormat<WritableComparable<?>, HCatRecord> {
        
        /*
         * @see
         * org.apache.hadoop.mapred.OutputFormat#checkOutputSpecs(org.apache
         * .hadoop .fs.FileSystem, org.apache.hadoop.mapred.JobConf)
         */
        @Override
        public void checkOutputSpecs(FileSystem fs, JobConf jobconf)
                throws IOException {
            throw new IOException("This operation is not supported.");
            
        }
        
        /*
         * @see
         * org.apache.hadoop.mapred.OutputFormat#getRecordWriter(org.apache.
         * hadoop .fs.FileSystem, org.apache.hadoop.mapred.JobConf,
         * java.lang.String, org.apache.hadoop.util.Progressable)
         */
        @Override
        public RecordWriter<WritableComparable<?>, HCatRecord> getRecordWriter(
                FileSystem fs, JobConf jobconf, String str,
                Progressable progress) throws IOException {
            throw new IOException("This operation is not supported.");
        }
        
        /*
         * @see
         * org.apache.hadoop.hive.ql.io.HiveOutputFormat#getHiveRecordWriter(org
         * .apache.hadoop.mapred.JobConf, org.apache.hadoop.fs.Path,
         * java.lang.Class, boolean, java.util.Properties,
         * org.apache.hadoop.util.Progressable)
         */
        @Override
        public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
                JobConf jc, Path finalOutPath,
                Class<? extends Writable> valueClass, boolean isCompressed,
                Properties tableProperties, Progressable progress)
                throws IOException {
            throw new IOException("This operation is not supported.");
        }
        
    }
    
}
