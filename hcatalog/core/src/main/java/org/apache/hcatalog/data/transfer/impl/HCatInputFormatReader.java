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

package org.apache.hcatalog.data.transfer.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.transfer.HCatReader;
import org.apache.hcatalog.data.transfer.ReadEntity;
import org.apache.hcatalog.data.transfer.ReaderContext;
import org.apache.hcatalog.data.transfer.state.StateProvider;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.shims.HCatHadoopShims;

/**
 * This reader reads via {@link HCatInputFormat}
 *
 */
public class HCatInputFormatReader extends HCatReader {

    private InputSplit split;

    public HCatInputFormatReader(InputSplit split, Configuration config,
                                 StateProvider sp) {
        super(config, sp);
        this.split = split;
    }

    public HCatInputFormatReader(ReadEntity info, Map<String, String> config) {
        super(info, config);
    }

    @Override
    public ReaderContext prepareRead() throws HCatException {
        try {
            Job job = new Job(conf);
            HCatInputFormat hcif = HCatInputFormat.setInput(
                job, re.getDbName(), re.getTableName()).setFilter(re.getFilterString());
            ReaderContext cntxt = new ReaderContext();
            cntxt.setInputSplits(hcif.getSplits(
                HCatHadoopShims.Instance.get().createJobContext(job.getConfiguration(), null)));
            cntxt.setConf(job.getConfiguration());
            return cntxt;
        } catch (IOException e) {
            throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED, e);
        } catch (InterruptedException e) {
            throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED, e);
        }
    }

    @Override
    public Iterator<HCatRecord> read() throws HCatException {

        HCatInputFormat inpFmt = new HCatInputFormat();
        RecordReader<WritableComparable, HCatRecord> rr;
        try {
            TaskAttemptContext cntxt = HCatHadoopShims.Instance.get().createTaskAttemptContext(conf, new TaskAttemptID());
            rr = inpFmt.createRecordReader(split, cntxt);
            rr.initialize(split, cntxt);
        } catch (IOException e) {
            throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED, e);
        } catch (InterruptedException e) {
            throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED, e);
        }
        return new HCatRecordItr(rr);
    }

    private static class HCatRecordItr implements Iterator<HCatRecord> {

        private RecordReader<WritableComparable, HCatRecord> curRecReader;

        HCatRecordItr(RecordReader<WritableComparable, HCatRecord> rr) {
            curRecReader = rr;
        }

        @Override
        public boolean hasNext() {
            try {
                boolean retVal = curRecReader.nextKeyValue();
                if (retVal) {
                    return true;
                }
                // if its false, we need to close recordReader.
                curRecReader.close();
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public HCatRecord next() {
            try {
                return curRecReader.getCurrentValue();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not allowed");
        }
    }
}
