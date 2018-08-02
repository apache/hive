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

package org.apache.hadoop.hive.registry;

import java.util.Arrays;
import java.util.Map;

/**
 * Exception that should be thrown when the data is invalid, or an exceptional situation or error occurs while validating data.
 * Examples of scenarios that can cause the validation logic to throw this exception are missing fields, invalid fields,
 * invalid or corrupted data for a field, etc... The application is expected to handle this exception. For example, it can
 * store invalid data into data storage, redirect invalid data into a particular handler, rethrow the exception, send an alert, etc...
 */
public class DataValidationException extends Exception {

    public DataValidationException(byte[] rawData) {
        this("Invalid raw data [" + Arrays.toString(rawData) + "]");
    }

    public DataValidationException(Map<String, Object> parsedData) {
        this("Invalid parsed data " + parsedData);
    }

    public DataValidationException() {
    }

    public DataValidationException(String message) {
        super(message);
    }

    public DataValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataValidationException(Throwable cause) {
        super(cause);
    }

    public DataValidationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
