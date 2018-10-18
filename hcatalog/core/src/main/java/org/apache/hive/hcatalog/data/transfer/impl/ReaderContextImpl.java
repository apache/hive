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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.data.transfer.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.mapreduce.HCatSplit;

/**
 * This class contains the list of {@link InputSplit}s obtained
 * at master node and the configuration.
 */
class ReaderContextImpl implements ReaderContext, Configurable {

    private static final long serialVersionUID = -2656468331739574367L;
    private List<InputSplit> splits;
    private Configuration conf;

    public ReaderContextImpl() {
        this.splits = new ArrayList<InputSplit>();
        this.conf = new Configuration();
    }

    void setInputSplits(final List<InputSplit> splits) {
        this.splits = splits;
    }

    List<InputSplit> getSplits() {
        return splits;
    }

    @Override
    public int numSplits() {
        return splits.size();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(final Configuration config) {
        conf = config;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        conf.write(out);
        out.writeInt(splits.size());
        for (InputSplit split : splits) {
            ((HCatSplit) split).write(out);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
        ClassNotFoundException {
        conf.readFields(in);
        int numOfSplits = in.readInt();
        for (int i = 0; i < numOfSplits; i++) {
            HCatSplit split = new HCatSplit();
            split.readFields(in);
            splits.add(split);
        }
    }
}
