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
package org.apache.hadoop.hive.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Collection;

/**
 * Utilities for different blob (object) storage systems
 */
public class BlobStorageUtils {
    private static final boolean DISABLE_BLOBSTORAGE_AS_SCRATCHDIR = false;

    public static boolean isBlobStoragePath(final Configuration conf, final Path path) {
        return path != null && isBlobStorageScheme(conf, path.toUri().getScheme());
    }

    public static boolean isBlobStorageFileSystem(final Configuration conf, final FileSystem fs) {
        return fs != null && isBlobStorageScheme(conf, fs.getScheme());
    }

    public static boolean isBlobStorageScheme(final Configuration conf, final String scheme) {
        Collection<String> supportedBlobStoreSchemes =
                conf.getStringCollection(HiveConf.ConfVars.HIVE_BLOBSTORE_SUPPORTED_SCHEMES.varname);

        return supportedBlobStoreSchemes.contains(scheme);
    }

    public static boolean isBlobStorageAsScratchDir(final Configuration conf) {
        return conf.getBoolean(
                HiveConf.ConfVars.HIVE_BLOBSTORE_USE_BLOBSTORE_AS_SCRATCHDIR.varname,
                DISABLE_BLOBSTORAGE_AS_SCRATCHDIR
        );
    }

    /**
     * Returns true if {@link HiveConf.ConfVars#HIVE_BLOBSTORE_OPTIMIZATIONS_ENABLED} is true, false otherwise.
     */
    public static boolean areOptimizationsEnabled(final Configuration conf) {
        return conf.getBoolean(
                HiveConf.ConfVars.HIVE_BLOBSTORE_OPTIMIZATIONS_ENABLED.varname,
                HiveConf.ConfVars.HIVE_BLOBSTORE_OPTIMIZATIONS_ENABLED.defaultBoolVal
        );
    }
}
