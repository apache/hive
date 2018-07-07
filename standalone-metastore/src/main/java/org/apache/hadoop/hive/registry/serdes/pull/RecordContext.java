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
package org.apache.hadoop.hive.registry.serdes.pull;

/**
 * {@link PullEventContext} instance indicating an event.
 */
public class RecordContext<F> implements PullEventContext<F> {

    private final boolean startRecord;
    private final boolean endRecord;

    protected RecordContext(boolean startRecord, boolean endRecord) {
        this.startRecord = startRecord;
        this.endRecord = endRecord;
        if (startRecord == endRecord) {
            throw new IllegalArgumentException("start-event and end-event can not be same.");
        }
    }

    @Override
    public boolean startRecord() {
        return startRecord;
    }

    @Override
    public boolean endRecord() {
        return endRecord;
    }

    @Override
    public boolean startField() {
        return false;
    }

    @Override
    public boolean endField() {
        return false;
    }

    @Override
    public F currentField() {
        return null;
    }

    @Override
    public FieldValue<F> fieldValue() {
        return null;
    }

}
