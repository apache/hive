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

package org.apache.hadoop.hive.metastore.utils;

import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

public final class SmallFilesWarningUtil {
    private SmallFilesWarningUtil() {}

    /** Default minimum number of files before we consider emitting the warning. */
    public static final long DEFAULT_MIN_FILES = 100L;

    /**
     * True if quick HMS stats in {@code parameters} indicate small average file size.
     * Missing/malformed stats or insufficient file count return false.
     */
    public static boolean smallAverageFilesDetected(long minFiles,
                                                    long avgSizeThreshold,
                                                    Map<String, String> parameters) {
        if (parameters == null) return false;

        final String ts = parameters.get(StatsSetupConst.TOTAL_SIZE);
        final String nf = parameters.get(StatsSetupConst.NUM_FILES);
        if (ts == null || nf == null) return false;

        final long totalSize;
        final long numFiles;
        try {
            totalSize = Long.parseLong(ts);
            numFiles  = Long.parseLong(nf);
        } catch (NumberFormatException ignore) {
            return false;
        }

        if (numFiles <= minFiles || totalSize <= 0) return false;

        final long avg = Math.floorDiv(totalSize, numFiles);
        return avg <= avgSizeThreshold;
    }

    /**
     * Convenience overload: reads the threshold from {@code conf} and uses {@link #DEFAULT_MIN_FILES}.
     * Returns false if {@code conf} is null.
     */
    public static boolean smallAverageFilesDetected(Configuration conf,
                                                    Map<String, String> parameters) {
        final long threshold = MetastoreConf.getLongVar(
                conf, MetastoreConf.ConfVars.MSCK_SMALLFILES_AVG_SIZE);
        return smallAverageFilesDetected(DEFAULT_MIN_FILES, threshold, parameters);
    }

    /**
     * Returns a formatted warning message if params indicate "small files", else Optional.empty().
     *
     * @param parameters table/partition parameters
     * @param minFiles   e.g. 100
     * @param avgSizeThreshold bytes, e.g. from conf
     * @param tableOrPartName preformatted table or partition name
     * @param tagPrefix  e.g. "[ANALYZE]" or "[ANALYZE][NOSCAN]"
     */
    public static Optional<String> smallFilesWarnings(Map<String, String> parameters,
                                                      long minFiles,
                                                      long avgSizeThreshold,
                                                      String tableOrPartName,
                                                      String tagPrefix) {
        if (!smallAverageFilesDetected(minFiles, avgSizeThreshold, parameters)) {
            return Optional.empty();
        }

        // At this point we know parsing succeeds; reparse to build the message.
        final long totalSize = Long.parseLong(parameters.get(StatsSetupConst.TOTAL_SIZE));
        final long numFiles  = Long.parseLong(parameters.get(StatsSetupConst.NUM_FILES));
        final long avg       = Math.floorDiv(totalSize, numFiles);

        final String prefix = (tagPrefix == null || tagPrefix.isEmpty()) ? "" : (tagPrefix + " ");
        final String msg = String.format(
                "%sSmall files detected: %s (avgBytes=%d, files=%d, totalBytes=%d)",
                prefix, tableOrPartName, avg, numFiles, totalSize);

        return Optional.of(msg);
    }
}
