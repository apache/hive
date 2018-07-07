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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class CompatibilityResult implements Serializable {
    public static final CompatibilityResult SUCCESS = new CompatibilityResult(true, null);

    private static final long serialVersionUID = 6611482356634821602L;

    private boolean compatible;
    private String errorMessage;
    private String errorLocation;
    private String schema;

    private CompatibilityResult() {
    }

    private CompatibilityResult(boolean compatible, String schema) {
        this.compatible = compatible;
        this.schema = schema;
    }

    private CompatibilityResult(boolean compatible, String errorMessage, String errorLocation, String schema) {
        this.compatible = compatible;
        this.errorMessage = errorMessage;
        this.errorLocation = errorLocation;
        this.schema = schema;
    }

    public boolean isCompatible() {
        return compatible;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getSchema() {
        return schema;
    }

    public String getErrorLocation() {
        return errorLocation;
    }

    @Override
    public String toString() {
        return "CompatibilityResult{" +
                "compatible=" + compatible +
                ", errorMessage='" + errorMessage + '\'' +
                ", errorLocation='" + errorLocation + '\'' +
                ", schema='" + schema + '\'' +
                '}';
    }

    /**
     * Returns {@link CompatibilityResult} instance with {@link CompatibilityResult#isCompatible()} as false and {@link CompatibilityResult#getErrorMessage()} as given {@code errorMessage}
     * @param errorMessage error message
     * @param errorLocation location of the error
     * @param schema schema for which this incompatibility result is returned.
     *
     */
    public static CompatibilityResult createIncompatibleResult(String errorMessage,
                                                               String errorLocation,
                                                               String schema) {
        return new CompatibilityResult(false, errorMessage, errorLocation, schema);
    }

    /**
     * @param schema schema to which compatibility is successful.
     * @return compatibility successful result for the given schema.
     */
    public static CompatibilityResult createCompatibleResult(String schema) {
        return new CompatibilityResult(true, schema);
    }
}
