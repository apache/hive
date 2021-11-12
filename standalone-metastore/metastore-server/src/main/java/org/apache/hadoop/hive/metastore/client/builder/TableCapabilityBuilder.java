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

package org.apache.hadoop.hive.metastore.client.builder;

/**
 * Builder for building table capabilities to be includes in TBLPROPERTIES
 * during createTable
 */
public class TableCapabilityBuilder {
    private String capabilitiesString = null;
    public static final String KEY_CAPABILITIES = "OBJCAPABILITIES";

    public TableCapabilityBuilder() {
        capabilitiesString = "";
    }

    public TableCapabilityBuilder add(String skill) {
        if (skill != null) {
            capabilitiesString += skill + ",";
        }
        return this;
    }

    public String build() {
        return this.capabilitiesString.substring(0, capabilitiesString.length() - 1);
    }

    public String getDBValue() {
        return KEY_CAPABILITIES + "=" + build();
    }

    public static String getKey() {
        return KEY_CAPABILITIES;
    }
}
