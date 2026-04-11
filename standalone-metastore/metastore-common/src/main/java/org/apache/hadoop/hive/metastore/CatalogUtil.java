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

public class CatalogUtil {
    // Catalog type constants
    public static final String NATIVE = "native";
    public static final String ICEBERG = "iceberg";

    public enum CatalogType {
        NATIVE,
        ICEBERG
    }

    /**
     * Check if the given catalog type name is valid.
     * @param name catalog type name (case-insensitive)
     * @return true if valid, false otherwise
     */
    public static boolean isValidCatalogType(String name) {
        if (name == null || name.trim().isEmpty()) {
            return false;
        }
        try {
            CatalogType.valueOf(name.toUpperCase().trim());
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Normalize catalog type to standard lowercase format.
     * @param name catalog type name (case-insensitive)
     * @return normalized catalog type in lowercase
     * @throws IllegalArgumentException if the catalog type is null, empty, or invalid
     */
    public static String normalizeCatalogType(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Catalog type cannot be null or empty");
        }
        if (!isValidCatalogType(name)) {
            throw new IllegalArgumentException("Invalid catalog type: " + name);
        }
        return name.trim().toLowerCase();
    }
}

