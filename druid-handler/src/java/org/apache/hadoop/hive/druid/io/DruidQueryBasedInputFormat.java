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
package org.apache.hadoop.hive.druid.io;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.LocatedSegmentDescriptor;
import org.apache.druid.query.Query;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.DruidStorageHandler;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.serde.DruidGroupByQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidScanQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidTimeseriesQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidTopNQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;

/**
 * Druid query based input format.
 *
 * Given a query and the Druid broker address, it will send it, and retrieve
 * and parse the results.
 */
public class DruidQueryBasedInputFormat extends InputFormat<NullWritable, DruidWritable>
        implements org.apache.hadoop.mapred.InputFormat<NullWritable, DruidWritable>, VectorizedInputFormatInterface {

  private static final Logger LOG = LoggerFactory.getLogger(DruidQueryBasedInputFormat.class);

  public static DruidQueryRecordReader getDruidQueryReader(String druidQueryType) {
    switch (druidQueryType) {
    case Query.TIMESERIES:
      return new DruidTimeseriesQueryRecordReader();
    case Query.TOPN:
      return new DruidTopNQueryRecordReader();
    case Query.GROUP_BY:
      return new DruidGroupByQueryRecordReader();
    case Query.SCAN:
      return new DruidScanQueryRecordReader();
    default:
      throw new IllegalStateException("Druid query type " + druidQueryType + " not recognized");
    }
  }

  @Override public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return getInputSplits(job);
  }

  @Override public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return Arrays.asList(getInputSplits(context.getConfiguration()));
  }

  protected HiveDruidSplit[] getInputSplits(Configuration conf) throws IOException {
    String address = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS);
    String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID);
    if (StringUtils.isEmpty(address)) {
      throw new IOException("Druid broker address not specified in configuration");
    }
    String druidQuery = StringEscapeUtils.unescapeJava(conf.get(Constants.DRUID_QUERY_JSON));

    String druidQueryType;
    if (StringUtils.isEmpty(druidQuery)) {
      // Empty, maybe because CBO did not run; we fall back to
      // full Select query
      LOG.warn("Druid query is empty; creating Select query");
      String dataSource = conf.get(Constants.DRUID_DATA_SOURCE);
      if (dataSource == null || dataSource.isEmpty()) {
        throw new IOException("Druid data source cannot be empty or null");
      }
      druidQuery = DruidStorageHandlerUtils.createScanAllQuery(dataSource, Utilities.getColumnNames(conf));
      druidQueryType = Query.SCAN;
      conf.set(Constants.DRUID_QUERY_TYPE, druidQueryType);
    } else {
      druidQueryType = conf.get(Constants.DRUID_QUERY_TYPE);
      if (druidQueryType == null) {
        throw new IOException("Druid query type not recognized");
      }
    }

    // Add Hive Query ID to Druid Query
    if (queryId != null) {
      druidQuery = withQueryId(druidQuery, queryId);
    }

    // hive depends on FileSplits
    Job job = Job.getInstance(conf);
    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
    Path[] paths = FileInputFormat.getInputPaths(jobContext);

    // We need to deserialize and serialize query so intervals are written in the JSON
    // Druid query with user timezone, as this is default Hive time semantics.
    // Then, create splits with the Druid queries.
    switch (druidQueryType) {
    case Query.TIMESERIES:
    case Query.TOPN:
    case Query.GROUP_BY:
      return new HiveDruidSplit[] {new HiveDruidSplit(druidQuery, paths[0], new String[] {address})};
    case Query.SCAN:
      ScanQuery scanQuery = DruidStorageHandlerUtils.JSON_MAPPER.readValue(druidQuery, ScanQuery.class);
      return distributeScanQuery(address, scanQuery, paths[0]);
    default:
      throw new IOException("Druid query type not recognized");
    }
  }

  /* New method that distributes the Scan query by creating splits containing
   * information about different Druid nodes that have the data for the given
   * query. */
  private static HiveDruidSplit[] distributeScanQuery(String address, ScanQuery query, Path dummyPath)
      throws IOException {
    // If it has a limit, we use it and we do not distribute the query
    final boolean isFetch = query.getScanRowsLimit() < Long.MAX_VALUE;
    if (isFetch) {
      return new HiveDruidSplit[] {new HiveDruidSplit(DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(query),
          dummyPath,
          new String[] {address})
      };
    }

    final List<LocatedSegmentDescriptor> segmentDescriptors = fetchLocatedSegmentDescriptors(address, query);

    // Create one input split for each segment
    final int numSplits = segmentDescriptors.size();
    final HiveDruidSplit[] splits = new HiveDruidSplit[segmentDescriptors.size()];
    for (int i = 0; i < numSplits; i++) {
      final LocatedSegmentDescriptor locatedSD = segmentDescriptors.get(i);
      final String[] hosts = new String[locatedSD.getLocations().size() + 1];
      for (int j = 0; j < locatedSD.getLocations().size(); j++) {
        hosts[j] = locatedSD.getLocations().get(j).getHost();
      }
      // Default to broker if all other hosts fail.
      hosts[locatedSD.getLocations().size()] = address;

      // Create partial Select query
      final SegmentDescriptor
          newSD =
          new SegmentDescriptor(locatedSD.getInterval(), locatedSD.getVersion(), locatedSD.getPartitionNumber());
      final Query partialQuery = query.withQuerySegmentSpec(new MultipleSpecificSegmentSpec(Lists.newArrayList(newSD)));
      splits[i] =
          new HiveDruidSplit(DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(partialQuery), dummyPath, hosts);
    }
    return splits;
  }

  private static List<LocatedSegmentDescriptor> fetchLocatedSegmentDescriptors(String address, BaseQuery query)
      throws IOException {
    final String intervals = StringUtils.join(query.getIntervals(), ","); // Comma-separated intervals without brackets
    final String
        request =
        String.format("http://%s/druid/v2/datasources/%s/candidates?intervals=%s",
            address,
            query.getDataSource().getNames().get(0),
            URLEncoder.encode(intervals, "UTF-8"));
    LOG.debug("sending request {} to query for segments", request);
    final InputStream response;
    try {
      response =
          DruidStorageHandlerUtils.submitRequest(DruidStorageHandler.getHttpClient(),
              new Request(HttpMethod.GET, new URL(request)));
    } catch (Exception e) {
      throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }

    // Retrieve results
    final List<LocatedSegmentDescriptor> segmentDescriptors;
    try {
      segmentDescriptors =
          DruidStorageHandlerUtils.JSON_MAPPER.readValue(response, new TypeReference<List<LocatedSegmentDescriptor>>() {
          });
    } catch (Exception e) {
      response.close();
      throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    return segmentDescriptors;
  }

  private static String withQueryId(String druidQuery, String queryId) throws IOException {
    Query<?> queryWithId = DruidStorageHandlerUtils.JSON_MAPPER.readValue(druidQuery, BaseQuery.class).withId(queryId);
    return DruidStorageHandlerUtils.JSON_MAPPER.writeValueAsString(queryWithId);
  }

  @Override public org.apache.hadoop.mapred.RecordReader<NullWritable, DruidWritable> getRecordReader(
      org.apache.hadoop.mapred.InputSplit split,
      JobConf job,
      Reporter reporter) throws IOException {
    // We need to provide a different record reader for every type of Druid query.
    // The reason is that Druid results format is different for each type.
    final DruidQueryRecordReader<?> reader;
    // By default, we use druid scan query as fallback.
    final String druidQueryType = job.get(Constants.DRUID_QUERY_TYPE, Query.SCAN);
    reader = getDruidQueryReader(druidQueryType);
    reader.initialize((HiveDruidSplit) split, job);
    if (Utilities.getIsVectorized(job)) {
      //noinspection unchecked
      return (org.apache.hadoop.mapred.RecordReader) new DruidVectorizedWrapper(reader, job);
    }
    return reader;
  }

  @Override public RecordReader<NullWritable, DruidWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    // By default, we use druid scan query as fallback.
    final String druidQueryType = context.getConfiguration().get(Constants.DRUID_QUERY_TYPE, Query.SCAN);
    // We need to provide a different record reader for every type of Druid query.
    // The reason is that Druid results format is different for each type.
    //noinspection unchecked
    return getDruidQueryReader(druidQueryType);
  }

  @Override public VectorizedSupport.Support[] getSupportedFeatures() {
    return new VectorizedSupport.Support[0];
  }
}
