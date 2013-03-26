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

package org.apache.hcatalog.mapreduce;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Progressable;

class ProgressReporter implements  Reporter {

    private Progressable progressable;

    public ProgressReporter(TaskAttemptContext context) {
            this(context instanceof TaskInputOutputContext?
                    (TaskInputOutputContext)context:
                    Reporter.NULL);
    }

    public ProgressReporter(Progressable progressable) {
        this.progressable = progressable;
    }

    @Override
    public void setStatus(String status) {
    }

    @Override
    public Counters.Counter getCounter(Enum<?> name) {
        return Reporter.NULL.getCounter(name);
    }

    @Override
    public Counters.Counter getCounter(String group, String name) {
        return Reporter.NULL.getCounter(group,name);
    }

    @Override
    public void incrCounter(Enum<?> key, long amount) {
    }

    @Override
    public void incrCounter(String group, String counter, long amount) {
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
        return Reporter.NULL.getInputSplit();
    }

    @Override
    public void progress() {
        progressable.progress();
    }
}
