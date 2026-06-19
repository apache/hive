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
package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;



public class NullMROutput extends MROutput {

    private static final Logger LOG = LoggerFactory.getLogger(NullMROutput.class);

    public NullMROutput(OutputContext outputContext, int numPhysicalOutputs) {
        super(outputContext, numPhysicalOutputs);
    }

    @Override
    public List<Event> initialize() throws IOException, InterruptedException {
        List<Event> events = initializeBase();
        return events;
    }

    /**
     * Get a key value write to write Map Reduce compatible output
     */
    @Override
    public KeyValueWriter getWriter() throws IOException {
        return new KeyValueWriter() {
            @SuppressWarnings("unchecked")
            @Override
            public void write(Object key, Object value) throws IOException {
                throw new IOException("NullMROutput is not configured for actual rows");
            }
        };
    }

    /**
     * Call this in the processor before finishing to ensure outputs that
     * outputs have been flushed. Must be called before commit.
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
    }
}
