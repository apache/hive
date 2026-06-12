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

package org.apache.hadoop.hive.metastore;

import com.google.common.base.Strings;

public class CatalogUtil {
    public static final String TYPE = "type";

    // Supported Catalog types
    public enum CatalogType {
        HIVE,
        ICEBERG
    }

    /**
     * Check if the given catalog type is valid.
     * @param type catalog type (case-insensitive)
     * @return true if valid, false otherwise
     */
    public static boolean isValidCatalogType(String type) {
        try {
            CatalogType.valueOf(type.toUpperCase().trim());
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Normalize catalog type to standard lowercase format.
     * @param type catalog type (case-insensitive)
     * @return normalized catalog type in lowercase
     * @throws IllegalArgumentException if the catalog type is null, empty, or invalid
     */
    public static String normalizeCatalogType(String type) {
        if (Strings.isNullOrEmpty(type)) {
            // Set default type to hive if not specified
            return CatalogType.HIVE.name();
        }
        if (!isValidCatalogType(type)) {
            throw new IllegalArgumentException("Invalid catalog type: " + type);
        }
        return type.trim().toLowerCase();
    }
}

