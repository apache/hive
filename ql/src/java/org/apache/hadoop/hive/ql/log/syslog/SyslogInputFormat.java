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

package org.apache.hadoop.hive.ql.log.syslog;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Syslog input format handles files written to sys.logs hive table. This input format can prune files based on
 * predicates in ts (timestamp) column. Following are the assumptions/supported options for the file pruning to work.
 * When configuring log shippers (like fluentd), the following directory layout has to be followed
 *
 * WAREHOUSE_DIR/sys.db/logs/dt=DATE/ns=NAMESPACE/app=APPLICATION/YYYY-mm-DD-HH-MM_i[.log][.gz]
 * WAREHOUSE_DIR - is hive warehouse location for external tables
 * DATE - date of the log files
 * NAMESPACE - namespace for the applications
 * APPLICATION - name of the application (hiveserver2, metastore etc.)
 * YYYY - year
 * mm - month
 * DD - day of the month
 * HH - hour of the day (24 hr format)
 * MM - minute
 * i - index (log rotation)
 * [.log] - optional log extension
 * [.gz] - optional gz compression extension
 *
 * When executing queries like
 * select count(*) from sys.logs where ts between current_timestamp() - INTERVAL '1' DAY and current_timestamp()
 *
 * syslog input format can prunes the file listing based on the query filter condition.
 * File Pruning Assumptions/Support:
 * - All timestamps are in UTC (timestamp in file name, log lines and in query predicates all assume UTC)
 * - File names are in YYYY-mm-DD-HH-MM_i[.log][.gz] format.
 * - Seconds and millis in predicates gets rounded off to nearest minute (essentially ignoring them).
 * - Time slice used by log aggregation library should match with hive.syslog.input.format.file.time.slice config.
 *   If a filename is 2019-04-02-21-00_0.log.gz and timeslice is 300s then the file 2019-04-02-21-00_0.log.gz is
 *   expected to have log lines from timestamp 2019:04:02 21:00:00 to 2019:04:02 21:05:00 timestamp.
 * - Logs table should have 'ts' as timestamp column.
 * - Only simple BETWEEN filter predicate is supported for 'ts' column. There cannot be &gt;1 predicates on 'ts' column.
 */
public class SyslogInputFormat extends TextInputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(SyslogInputFormat.class);
  private static final long MILLISECONDS_PER_MINUTE = 60 * 1000L;
  private SearchArgument sarg;
  private JobConf jobConf;
  private ProjectionPusher projectionPusher = new ProjectionPusher();

  enum Location {
    BEFORE,
    MIN,
    MIDDLE,
    MAX,
    AFTER
  }

  @Override
  protected FileStatus[] listStatus(final JobConf job) throws IOException {
    FileStatus[] fileStatuses = super.listStatus(job);
    String filterExprSerialized = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    final boolean filePruning = HiveConf.getBoolVar(job, HiveConf.ConfVars.SYSLOG_INPUT_FORMAT_FILE_PRUNING);
    final long timeSliceSeconds = HiveConf.getTimeVar(job, HiveConf.ConfVars.SYSLOG_INPUT_FORMAT_FILE_TIME_SLICE,
      TimeUnit.SECONDS);
    if (filePruning && filterExprSerialized != null) {
      ExprNodeGenericFuncDesc filterExpr = SerializationUtilities.deserializeExpression(filterExprSerialized);
      // we receive the expression filters from hive and convert it to sarg keeping only timestamp (ts) column from
      // sys.logs table. The SARG on ts column is evaluated against the files that are formatted in YYYY-mm-dd-MM_* format
      this.sarg = ConvertAstToSearchArg.create(job, filterExpr);
    }
    LOG.info("Syslog configs - filePruning: {} SARG: {} timeSliceSeconds: {}", filePruning, sarg, timeSliceSeconds);
    if (sarg != null) {
      fileStatuses = pruneFiles(sarg, timeSliceSeconds, Arrays.asList(fileStatuses)).toArray(new FileStatus[0]);
    }
    return fileStatuses;
  }

  @VisibleForTesting
  public List<FileStatus> pruneFiles(final SearchArgument sarg, final long timeSliceSeconds,
    final List<FileStatus> inputFiles) {
    int tsExpr = 0;
    List<PredicateLeaf> predLeaves = sarg.getLeaves();
    SearchArgument.TruthValue[] truthValues = new SearchArgument.TruthValue[predLeaves.size()];
    PredicateLeaf tsPred = null;
    for (int i = 0; i < truthValues.length; i++) {
      if (!predLeaves.get(i).getColumnName().equalsIgnoreCase("ts")) {
        truthValues[i] = SearchArgument.TruthValue.YES_NO_NULL;
      } else {
        tsPred = predLeaves.get(i);
        tsExpr++;
      }
    }
    // multiple expressions on 'ts' columns are not supported
    if (tsExpr == 0 || tsExpr > 1) {
      // no expressions on timestamp column or multi-expression
      if (tsExpr == 0) {
        LOG.warn("No filter expression on 'ts' column. Skipping file pruning..");
      } else {
        LOG.warn("Multi-filter expression ({}) on 'ts' column is not supported. Skipping file pruning..", tsExpr);
      }
      return inputFiles;
    }

    // only BETWEEN filter is supported
    if (tsPred.getOperator() == PredicateLeaf.Operator.BETWEEN) {
      List<FileStatus> selectedFiles = new ArrayList<>();
      List<Object> objects = tsPred.getLiteralList();

      Timestamp left = (Timestamp) objects.get(0);
      Timestamp right = (Timestamp) objects.get(1);
      // file pruning does not handle seconds and millis granularity.
      // so we round to nearest minute (left = floor) and (right = ceil).
      Timestamp predLowerBound = roundupToMinuteFloor(left);
      Timestamp predUpperBound = roundupToMinuteCeil(right);

      // we have left and right values from BETWEEN expression. For each file we have a start timestamp (from filename)
      // and end timestamp (start + timeslice). We compare the predicates with start and end timestamp of each file
      // and see if it falls within the range.
      for (FileStatus fileStatus : inputFiles) {
        Timestamp fileLowerBound = getTimeStampFromPath(fileStatus);
        if (fileLowerBound != null) {
          Timestamp fileUpperBound = Timestamp.from(fileLowerBound.toInstant().plusSeconds(timeSliceSeconds));
          Location predLowerLocation;
          Location predUpperLocation = null;
          predLowerLocation = compareToRange(predLowerBound, fileLowerBound, fileUpperBound);
          boolean selected = false;
          if (predLowerLocation == Location.BEFORE || predLowerLocation == Location.MIN) {
            predUpperLocation = compareToRange(predUpperBound, fileLowerBound, fileUpperBound);
            if (predUpperLocation == Location.AFTER || predUpperLocation == Location.MAX) {
              selectedFiles.add(fileStatus);
              selected = true;
            } else if (predUpperLocation == Location.BEFORE) {
              // file skipped (out of range)
            } else {
              selectedFiles.add(fileStatus);
              selected = true;
            }
          } else if (predLowerLocation == Location.AFTER) {
            // file skipped (out of range)
          } else {
            selectedFiles.add(fileStatus);
            selected = true;
          }

          LOG.info("file: {} -> [{}, {}] against predicate [{}({}), {}({})]. selected? {}",
            fileStatus.getPath(), fileLowerBound.toInstant(), fileUpperBound.toInstant(),
            predLowerBound.toInstant(), predLowerLocation, predUpperBound.toInstant(), predUpperLocation, selected);
        } else {
          LOG.warn("Timestamp cannot be extracted from filename. Incorrect file name convention? {}",
            fileStatus.getPath());
          return inputFiles;
        }
      }
      LOG.info("Total selected files: {}", selectedFiles.size());
      return selectedFiles;
    }
    LOG.warn("Unsupported expression ({}) on 'ts' column. Skipping file pruning..", tsPred.getOperator());
    return inputFiles;
  }

  // 2019-10-13 10:15:35.100 -> 2019-10-13 10:15:00.000
  private Timestamp roundupToMinuteFloor(final Timestamp ts) {
    long millis = ts.getTime();
    long newMillis = MILLISECONDS_PER_MINUTE * (millis / MILLISECONDS_PER_MINUTE);
    return new Timestamp(newMillis);
  }

  // 2019-10-13 10:15:35.100 -> 2019-10-13 10:16:00.000
  private Timestamp roundupToMinuteCeil(final Timestamp ts) {
    long millis = ts.getTime();
    long newMillis = MILLISECONDS_PER_MINUTE * (millis / MILLISECONDS_PER_MINUTE);
    newMillis += MILLISECONDS_PER_MINUTE;
    return new Timestamp(newMillis);
  }

  // 2019-04-02-21-00_0.log.gz -> 2019:04:02 21:00:00.0
  private static Timestamp getTimeStampFromPath(final FileStatus fileStatus) {
    String file = fileStatus.getPath().getName();
    if (file.contains("_")) {
      String tsStr = file.split("_")[0];
      if (tsStr.contains("-")) {
        String[] tsTokens = tsStr.split("-");
        if (tsTokens.length == 5) {
          try {
            Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.getDefault());
            int year = Integer.parseUnsignedInt(tsTokens[0]);
            int month = Integer.parseUnsignedInt(tsTokens[1]);
            int day = Integer.parseUnsignedInt(tsTokens[2]);
            int hour = Integer.parseUnsignedInt(tsTokens[3]);
            int min = Integer.parseUnsignedInt(tsTokens[4]);
            // month value is zero indexed
            calendar.set(year, month - 1, day, hour, min, 0);
            Timestamp ts = new Timestamp(calendar.getTimeInMillis());
            ts.setNanos(0);
            return ts;
          } catch (NumberFormatException e) {
            return null;
          }
        }
      }
    }
    return null;
  }

  private <T> Location compareToRange(Comparable<T> point, T min, T max) {
    int minCompare = point.compareTo(min);
    if (minCompare < 0) {
      return Location.BEFORE;
    } else if (minCompare == 0) {
      return Location.MIN;
    } else {
      int maxCompare = point.compareTo(max);
      if (maxCompare > 0) {
        return Location.AFTER;
      } else {
        return maxCompare == 0 ? Location.MAX : Location.MIDDLE;
      }
    }
  }

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(final InputSplit genericSplit, final JobConf job,
    final Reporter reporter)
    throws IOException {
    if (genericSplit instanceof FileSplit) {
      final Path finalPath = ((FileSplit) genericSplit).getPath();
      LOG.debug("Returning record reader for path {}", finalPath);
      jobConf = projectionPusher.pushProjectionsAndFilters(job, finalPath.getParent());
    }
    // textIF considers '\r' or '\n' as line ending but syslog uses '\r' for escaping new lines. So to read multi-line
    // exceptions correctly we explicitly use only '\n'
    jobConf.set("textinputformat.record.delimiter", "\n");
    return super.getRecordReader(genericSplit, jobConf, reporter);
  }
}
