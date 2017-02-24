/**
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
package org.apache.hadoop.hive.druid.io;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.druid.DruidDateTimeUtils;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.serde.DruidGroupByQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidSelectQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidTimeseriesQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidTopNQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Lists;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.http.client.Request;

import io.druid.query.BaseQuery;
import io.druid.query.Druids;
import io.druid.query.Druids.SegmentMetadataQueryBuilder;
import io.druid.query.Druids.SelectQueryBuilder;
import io.druid.query.Druids.TimeBoundaryQueryBuilder;
import io.druid.query.LocatedSegmentDescriptor;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryResultValue;

/**
 * Druid query based input format.
 *
 * Given a query and the Druid broker address, it will send it, and retrieve
 * and parse the results.
 */
public class DruidQueryBasedInputFormat extends InputFormat<NullWritable, DruidWritable>
        implements org.apache.hadoop.mapred.InputFormat<NullWritable, DruidWritable> {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidQueryBasedInputFormat.class);

  @Override
  public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job, int numSplits)
          throws IOException {
    return getInputSplits(job);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return Arrays.<InputSplit>asList(getInputSplits(context.getConfiguration()));
  }

  @SuppressWarnings("deprecation")
  private HiveDruidSplit[] getInputSplits(Configuration conf) throws IOException {
    String address = HiveConf.getVar(conf,
            HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS
    );
    if (StringUtils.isEmpty(address)) {
      throw new IOException("Druid broker address not specified in configuration");
    }
    String druidQuery = StringEscapeUtils.unescapeJava(conf.get(Constants.DRUID_QUERY_JSON));
    String druidQueryType;
    if (StringUtils.isEmpty(druidQuery)) {
      // Empty, maybe because CBO did not run; we fall back to
      // full Select query
      if (LOG.isWarnEnabled()) {
        LOG.warn("Druid query is empty; creating Select query");
      }
      String dataSource = conf.get(Constants.DRUID_DATA_SOURCE);
      if (dataSource == null) {
        throw new IOException("Druid data source cannot be empty");
      }
      druidQuery = createSelectStarQuery(dataSource);
      druidQueryType = Query.SELECT;
    } else {
      druidQueryType = conf.get(Constants.DRUID_QUERY_TYPE);
      if (druidQueryType == null) {
        throw new IOException("Druid query type not recognized");
      }
    }

    // hive depends on FileSplits
    Job job = new Job(conf);
    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
    Path[] paths = FileInputFormat.getInputPaths(jobContext);

    // We need to deserialize and serialize query so intervals are written in the JSON
    // Druid query with user timezone, as this is default Hive time semantics.
    // Then, create splits with the Druid queries.
    switch (druidQueryType) {
      case Query.TIMESERIES:
      case Query.TOPN:
      case Query.GROUP_BY:
        return new HiveDruidSplit[] { new HiveDruidSplit(deserializeSerialize(druidQuery),
                paths[0], new String[] {address}) };
      case Query.SELECT:
        SelectQuery selectQuery = DruidStorageHandlerUtils.JSON_MAPPER.readValue(
                druidQuery, SelectQuery.class);
        boolean distributed = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_DRUID_SELECT_DISTRIBUTE);
        if (distributed) {
          return distributeSelectQuery(conf, address, selectQuery, paths[0]);
        } else {
          return splitSelectQuery(conf, address, selectQuery, paths[0]);
        }
      default:
        throw new IOException("Druid query type not recognized");
    }
  }

  private static String createSelectStarQuery(String dataSource) throws IOException {
    // Create Select query
    SelectQueryBuilder builder = new Druids.SelectQueryBuilder();
    builder.dataSource(dataSource);
    builder.intervals(Arrays.asList(DruidTable.DEFAULT_INTERVAL));
    builder.pagingSpec(PagingSpec.newSpec(1));
    Map<String, Object> context = new HashMap<>();
    context.put(Constants.DRUID_QUERY_FETCH, false);
    builder.context(context);
    return DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(builder.build());
  }

  /* New method that distributes the Select query by creating splits containing
   * information about different Druid nodes that have the data for the given
   * query. */
  private static HiveDruidSplit[] distributeSelectQuery(Configuration conf, String address,
      SelectQuery query, Path dummyPath) throws IOException {
    // If it has a limit, we use it and we do not distribute the query
    final boolean isFetch = query.getContextBoolean(Constants.DRUID_QUERY_FETCH, false);
    if (isFetch) {
      return new HiveDruidSplit[] { new HiveDruidSplit(
              DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(query), dummyPath,
              new String[]{address} ) };
    }

    // Properties from configuration
    final int numConnection = HiveConf.getIntVar(conf,
            HiveConf.ConfVars.HIVE_DRUID_NUM_HTTP_CONNECTION);
    final Period readTimeout = new Period(
            HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DRUID_HTTP_READ_TIMEOUT));

    // Create request to obtain nodes that are holding data for the given datasource and intervals
    final Lifecycle lifecycle = new Lifecycle();
    final HttpClient client = HttpClientInit.createClient(
            HttpClientConfig.builder().withNumConnections(numConnection)
                    .withReadTimeout(readTimeout.toStandardDuration()).build(), lifecycle);
    try {
      lifecycle.start();
    } catch (Exception e) {
      LOG.error("Lifecycle start issue");
      throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    final String intervals =
            StringUtils.join(query.getIntervals(), ","); // Comma-separated intervals without brackets
    final String request = String.format(
            "http://%s/druid/v2/datasources/%s/candidates?intervals=%s",
            address, query.getDataSource().getNames().get(0), intervals);
    final InputStream response;
    try {
      response = DruidStorageHandlerUtils.submitRequest(client, new Request(HttpMethod.GET, new URL(request)));
    } catch (Exception e) {
      lifecycle.stop();
      throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }

    // Retrieve results
    final List<LocatedSegmentDescriptor> segmentDescriptors;
    try {
      segmentDescriptors = DruidStorageHandlerUtils.JSON_MAPPER.readValue(response,
              new TypeReference<List<LocatedSegmentDescriptor>>() {});
    } catch (Exception e) {
      response.close();
      throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    } finally {
      lifecycle.stop();
    }

    // Create one input split for each segment
    final int numSplits = segmentDescriptors.size();
    final HiveDruidSplit[] splits = new HiveDruidSplit[segmentDescriptors.size()];
    for (int i = 0; i < numSplits; i++) {
      final LocatedSegmentDescriptor locatedSD = segmentDescriptors.get(i);
      final String[] hosts = new String[locatedSD.getLocations().size()];
      for (int j = 0; j < locatedSD.getLocations().size(); j++) {
        hosts[j] = locatedSD.getLocations().get(j).getHost();
      }
      // Create partial Select query
      final SegmentDescriptor newSD = new SegmentDescriptor(
              locatedSD.getInterval(), locatedSD.getVersion(), locatedSD.getPartitionNumber());
      final SelectQuery partialQuery = query.withQuerySegmentSpec(
              new MultipleSpecificSegmentSpec(Lists.newArrayList(newSD)));
      splits[i] = new HiveDruidSplit(DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(partialQuery),
              dummyPath, hosts);
    }
    return splits;
  }

  /* Method that splits Select query depending on the threshold so read can be
   * parallelized. We will only contact the Druid broker to obtain all results. */
  private static HiveDruidSplit[] splitSelectQuery(Configuration conf, String address,
          SelectQuery query, Path dummyPath
  ) throws IOException {
    final int selectThreshold = (int) HiveConf.getIntVar(
            conf, HiveConf.ConfVars.HIVE_DRUID_SELECT_THRESHOLD);
    final int numConnection = HiveConf
            .getIntVar(conf, HiveConf.ConfVars.HIVE_DRUID_NUM_HTTP_CONNECTION);
    final Period readTimeout = new Period(
            HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DRUID_HTTP_READ_TIMEOUT));

    final boolean isFetch = query.getContextBoolean(Constants.DRUID_QUERY_FETCH, false);
    if (isFetch) {
      // If it has a limit, we use it and we do not split the query
      return new HiveDruidSplit[] { new HiveDruidSplit(
              DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(query), dummyPath,
              new String[] {address} ) };
    }

    // We do not have the number of rows, thus we need to execute a
    // Segment Metadata query to obtain number of rows
    SegmentMetadataQueryBuilder metadataBuilder = new Druids.SegmentMetadataQueryBuilder();
    metadataBuilder.dataSource(query.getDataSource());
    metadataBuilder.intervals(query.getIntervals());
    metadataBuilder.merge(true);
    metadataBuilder.analysisTypes();
    SegmentMetadataQuery metadataQuery = metadataBuilder.build();
    Lifecycle lifecycle = new Lifecycle();
    HttpClient client = HttpClientInit.createClient(
            HttpClientConfig.builder().withNumConnections(numConnection)
                    .withReadTimeout(readTimeout.toStandardDuration()).build(), lifecycle);
    try {
      lifecycle.start();
    } catch (Exception e) {
      LOG.error("Lifecycle start issue");
      throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    InputStream response;
    try {
      response = DruidStorageHandlerUtils.submitRequest(client,
              DruidStorageHandlerUtils.createRequest(address, metadataQuery)
      );
    } catch (Exception e) {
      lifecycle.stop();
      throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }

    // Retrieve results
    List<SegmentAnalysis> metadataList;
    try {
      metadataList = DruidStorageHandlerUtils.SMILE_MAPPER.readValue(response,
              new TypeReference<List<SegmentAnalysis>>() {
              }
      );
    } catch (Exception e) {
      response.close();
      throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    } finally {
      lifecycle.stop();
    }
    if (metadataList == null) {
      throw new IOException("Connected to Druid but could not retrieve datasource information");
    }
    if (metadataList.isEmpty()) {
      // There are no rows for that time range, we can submit query as it is
      return new HiveDruidSplit[] { new HiveDruidSplit(
              DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(query), dummyPath,
              new String[] {address} ) };
    }
    if (metadataList.size() != 1) {
      throw new IOException("Information about segments should have been merged");
    }

    final long numRows = metadataList.get(0).getNumRows();

    query = query.withPagingSpec(PagingSpec.newSpec(Integer.MAX_VALUE));
    if (numRows <= selectThreshold) {
      // We are not going to split it
      return new HiveDruidSplit[] { new HiveDruidSplit(
              DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(query), dummyPath,
              new String[] {address} ) };
    }

    // If the query does not specify a timestamp, we obtain the total time using
    // a Time Boundary query. Then, we use the information to split the query
    // following the Select threshold configuration property
    final List<Interval> intervals = new ArrayList<>();
    if (query.getIntervals().size() == 1 && query.getIntervals().get(0).withChronology(
            ISOChronology.getInstanceUTC()).equals(DruidTable.DEFAULT_INTERVAL)) {
      // Default max and min, we should execute a time boundary query to get a
      // more precise range
      TimeBoundaryQueryBuilder timeBuilder = new Druids.TimeBoundaryQueryBuilder();
      timeBuilder.dataSource(query.getDataSource());
      TimeBoundaryQuery timeQuery = timeBuilder.build();

      lifecycle = new Lifecycle();
      client = HttpClientInit.createClient(
              HttpClientConfig.builder().withNumConnections(numConnection)
                      .withReadTimeout(readTimeout.toStandardDuration()).build(), lifecycle);
      try {
        lifecycle.start();
      } catch (Exception e) {
        LOG.error("Lifecycle start issue");
        throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
      try {
        response = DruidStorageHandlerUtils.submitRequest(client,
                DruidStorageHandlerUtils.createRequest(address, timeQuery)
        );
      } catch (Exception e) {
        lifecycle.stop();
        throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
      }

      // Retrieve results
      List<Result<TimeBoundaryResultValue>> timeList;
      try {
        timeList = DruidStorageHandlerUtils.SMILE_MAPPER.readValue(response,
                new TypeReference<List<Result<TimeBoundaryResultValue>>>() {
                }
        );
      } catch (Exception e) {
        response.close();
        throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
      } finally {
        lifecycle.stop();
      }
      if (timeList == null || timeList.isEmpty()) {
        throw new IOException(
                "Connected to Druid but could not retrieve time boundary information");
      }
      if (timeList.size() != 1) {
        throw new IOException("We should obtain a single time boundary");
      }

      intervals.add(new Interval(timeList.get(0).getValue().getMinTime().getMillis(),
              timeList.get(0).getValue().getMaxTime().getMillis(), ISOChronology.getInstanceUTC()
      ));
    } else {
      intervals.addAll(query.getIntervals());
    }

    // Create (numRows/default threshold) input splits
    int numSplits = (int) Math.ceil((double) numRows / selectThreshold);
    List<List<Interval>> newIntervals = createSplitsIntervals(intervals, numSplits);
    HiveDruidSplit[] splits = new HiveDruidSplit[numSplits];
    for (int i = 0; i < numSplits; i++) {
      // Create partial Select query
      final SelectQuery partialQuery = query.withQuerySegmentSpec(
              new MultipleIntervalSegmentSpec(newIntervals.get(i)));
      splits[i] = new HiveDruidSplit(
              DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(partialQuery), dummyPath,
              new String[] {address});
    }
    return splits;
  }

  private static List<List<Interval>> createSplitsIntervals(List<Interval> intervals, int numSplits
  ) {
    final long totalTime = DruidDateTimeUtils.extractTotalTime(intervals);
    long startTime = intervals.get(0).getStartMillis();
    long endTime = startTime;
    long currTime = 0;
    List<List<Interval>> newIntervals = new ArrayList<>();
    for (int i = 0, posIntervals = 0; i < numSplits; i++) {
      final long rangeSize = Math.round((double) (totalTime * (i + 1)) / numSplits) -
              Math.round((double) (totalTime * i) / numSplits);
      // Create the new interval(s)
      List<Interval> currentIntervals = new ArrayList<>();
      while (posIntervals < intervals.size()) {
        final Interval interval = intervals.get(posIntervals);
        final long expectedRange = rangeSize - currTime;
        if (interval.getEndMillis() - startTime >= expectedRange) {
          endTime = startTime + expectedRange;
          currentIntervals.add(new Interval(startTime, endTime, ISOChronology.getInstanceUTC()));
          startTime = endTime;
          currTime = 0;
          break;
        }
        endTime = interval.getEndMillis();
        currentIntervals.add(new Interval(startTime, endTime, ISOChronology.getInstanceUTC()));
        currTime += (endTime - startTime);
        startTime = intervals.get(++posIntervals).getStartMillis();
      }
      newIntervals.add(currentIntervals);
    }
    assert endTime == intervals.get(intervals.size() - 1).getEndMillis();
    return newIntervals;
  }

  private static String deserializeSerialize(String druidQuery)
          throws JsonParseException, JsonMappingException, IOException {
    BaseQuery<?> deserializedQuery = DruidStorageHandlerUtils.JSON_MAPPER.readValue(
            druidQuery, BaseQuery.class);
    return DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(deserializedQuery);
  }

  @Override
  public org.apache.hadoop.mapred.RecordReader<NullWritable, DruidWritable> getRecordReader(
          org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter
  )
          throws IOException {
    // We need to provide a different record reader for every type of Druid query.
    // The reason is that Druid results format is different for each type.
    final DruidQueryRecordReader<?, ?> reader;
    final String druidQueryType = job.get(Constants.DRUID_QUERY_TYPE);
    if (druidQueryType == null) {
      reader = new DruidSelectQueryRecordReader(); // By default
      reader.initialize((HiveDruidSplit) split, job);
      return reader;
    }
    switch (druidQueryType) {
      case Query.TIMESERIES:
        reader = new DruidTimeseriesQueryRecordReader();
        break;
      case Query.TOPN:
        reader = new DruidTopNQueryRecordReader();
        break;
      case Query.GROUP_BY:
        reader = new DruidGroupByQueryRecordReader();
        break;
      case Query.SELECT:
        reader = new DruidSelectQueryRecordReader();
        break;
      default:
        throw new IOException("Druid query type not recognized");
    }
    reader.initialize((HiveDruidSplit) split, job);
    return reader;
  }

  @Override
  public RecordReader<NullWritable, DruidWritable> createRecordReader(InputSplit split,
          TaskAttemptContext context
  ) throws IOException, InterruptedException {
    // We need to provide a different record reader for every type of Druid query.
    // The reason is that Druid results format is different for each type.
    final String druidQueryType = context.getConfiguration().get(Constants.DRUID_QUERY_TYPE);
    if (druidQueryType == null) {
      return new DruidSelectQueryRecordReader(); // By default
    }
    final DruidQueryRecordReader<?, ?> reader;
    switch (druidQueryType) {
      case Query.TIMESERIES:
        reader = new DruidTimeseriesQueryRecordReader();
        break;
      case Query.TOPN:
        reader = new DruidTopNQueryRecordReader();
        break;
      case Query.GROUP_BY:
        reader = new DruidGroupByQueryRecordReader();
        break;
      case Query.SELECT:
        reader = new DruidSelectQueryRecordReader();
        break;
      default:
        throw new IOException("Druid query type not recognized");
    }
    return reader;
  }

}
