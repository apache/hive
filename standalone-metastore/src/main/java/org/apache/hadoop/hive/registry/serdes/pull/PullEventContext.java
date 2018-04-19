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
 * This class represents the current context of the parsed payload which can be of a new record or a new field.
 *
 * @param <F> type to represent field information in a schema
 */
public interface PullEventContext<F> {

    /**
     * @return true if this context indicates start of a record.
     */
    boolean startRecord();

    /**
     * @return true if this context indicates end of a record.
     */
    boolean endRecord();

    /**
     *
     * @return true if this context indicates start of a field. New field information can be retrieved by calling {@code #currentField()}.
     */
    boolean startField();

    /**
     * @return true if this context indicates end of field. New field and value can be retrieved by calling {@code #fieldValue()}.
     *
     */
    boolean endField();

    /**
     * @return Current field
     */
    F currentField();

    /**
     * @return {@link PullEventContext.FieldValue} instance for the current context.
     */
    FieldValue<F> fieldValue();

    /**
     * This class contains information about Field and its value.
     *
     * @param <F> type to represent field information in a schema
     */
    interface FieldValue<F> {

        /**
         * @return current field
         */
        F field();

        /**
         * This MAY be lazily generated. If this method is not accessed then internal stream may not realize the value
         * but stream can move to the next field location if it exists.
         *
         * @return the value of this field.
         */
        Object value();
    }
}
