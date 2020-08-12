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
package org.apache.hadoop.hive.druid.serde;

import com.google.common.collect.Iterators;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import io.druid.query.BaseQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.io.HiveDruidSplit;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Base record reader for given a Druid query. This class contains the logic to
 * send the query to the broker and retrieve the results. The transformation to
 * emit records needs to be done by the classes that extend the reader.
 *
 * The key for each record will be a NullWritable, while the value will be a
 * DruidWritable containing the timestamp as well as all values resulting from
 * the query.
 */
public abstract class DruidQueryRecordReader<T extends BaseQuery<R>, R extends Comparable<R>>
        extends RecordReader<NullWritable, DruidWritable>
        implements org.apache.hadoop.mapred.RecordReader<NullWritable, DruidWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(DruidQueryRecordReader.class);

  /**
   * Query that Druid executes.
   */
  protected T query;

  /**
   * Query results.
   */
  protected Iterator<R> results = Iterators.emptyIterator();

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    initialize(split, context.getConfiguration());
  }

  public void initialize(InputSplit split, Configuration conf) throws IOException {
    HiveDruidSplit hiveDruidSplit = (HiveDruidSplit) split;

    // Create query
    query = createQuery(hiveDruidSplit.getDruidQuery());

    // Execute query
    if (LOG.isInfoEnabled()) {
      LOG.info("Retrieving from druid using query:\n " + query);
    }

    final Lifecycle lifecycle = new Lifecycle();
    final int numConnection = HiveConf
            .getIntVar(conf, HiveConf.ConfVars.HIVE_DRUID_NUM_HTTP_CONNECTION);
    final Period readTimeout = new Period(
            HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DRUID_HTTP_READ_TIMEOUT));

    HttpClient client = HttpClientInit.createClient(
            HttpClientConfig.builder().withReadTimeout(readTimeout.toStandardDuration())
                    .withNumConnections(numConnection).build(), lifecycle);
    try {
      lifecycle.start();
    } catch (Exception e) {
      LOG.error("Issues with lifecycle start", e);
    }
    InputStream response;
    try {
      response = DruidStorageHandlerUtils.submitRequest(client,
              DruidStorageHandlerUtils.createRequest(hiveDruidSplit.getLocations()[0], query));
    } catch (Exception e) {
      lifecycle.stop();
      throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }

    // Retrieve results
    List<R> resultsList;
    try {
      resultsList = createResultsList(response);
    } catch (IOException e) {
      response.close();
      throw e;
    } finally {
      lifecycle.stop();
    }
    if (resultsList == null || resultsList.isEmpty()) {
      return;
    }
    results = resultsList.iterator();
  }

  protected abstract T createQuery(String content) throws IOException;

  protected abstract List<R> createResultsList(InputStream content) throws IOException;

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public DruidWritable createValue() {
    return new DruidWritable();
  }

  @Override
  public abstract boolean next(NullWritable key, DruidWritable value) throws IOException;

  @Override
  public long getPos() {
    return 0;
  }

  @Override
  public abstract boolean nextKeyValue() throws IOException;

  @Override
  public abstract NullWritable getCurrentKey() throws IOException, InterruptedException;

  @Override
  // TODO: we could generate vector row batches so that vectorized execution may get triggered
  public abstract DruidWritable getCurrentValue() throws IOException, InterruptedException;

  @Override
  public abstract float getProgress() throws IOException;

  @Override
  public void close() {
    // Nothing to do
  }

}
