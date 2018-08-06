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
package org.apache.hadoop.hive.druid.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.druid.DruidStorageHandler;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.io.HiveDruidSplit;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

  private HttpClient httpClient;
  private ObjectMapper mapper;
  // Smile mapper is used to read query results that are serialized as binary instead of json
  private ObjectMapper smileMapper;

  /**
   * Query that Druid executes.
   */
  protected Query query;

  /**
   * Query results as a streaming iterator.
   */
  protected JsonParserIterator<R> queryResultsIterator =  null;

  /**
   * Result type definition used to read the rows, this is query dependent.
   */
  protected JavaType resultsType = null;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    initialize(split, context.getConfiguration());
  }

  public void initialize(InputSplit split, Configuration conf, ObjectMapper mapper,
          ObjectMapper smileMapper, HttpClient httpClient
  ) throws IOException {
    HiveDruidSplit hiveDruidSplit = (HiveDruidSplit) split;
    Preconditions.checkNotNull(hiveDruidSplit, "input split is null ???");
    this.mapper = Preconditions.checkNotNull(mapper, "object Mapper can not be null");
    // Smile mapper is used to read query results that are serilized as binary instead of json
    this.smileMapper = Preconditions.checkNotNull(smileMapper, "Smile Mapper can not be null");
    // Create query
    this.query = this.mapper.readValue(Preconditions.checkNotNull(hiveDruidSplit.getDruidQuery()), Query.class);
    Preconditions.checkNotNull(query);
    this.resultsType = getResultTypeDef();
    this.httpClient = Preconditions.checkNotNull(httpClient, "need Http Client");
    // Execute query
    LOG.debug("Retrieving data from druid using query:\n " + query);
    final String address = hiveDruidSplit.getLocations()[0];
    if (Strings.isNullOrEmpty(address)) {
      throw new IOException("can not fetch results form empty or null host value");
    }
    Request request = DruidStorageHandlerUtils.createSmileRequest(address, query);
    Future<InputStream> inputStreamFuture = this.httpClient
            .go(request, new InputStreamResponseHandler());
    queryResultsIterator = new JsonParserIterator(this.smileMapper, resultsType, inputStreamFuture,
            request.getUrl().toString(), query
    );
  }

  public void initialize(InputSplit split, Configuration conf) throws IOException {
    initialize(split, conf, DruidStorageHandlerUtils.JSON_MAPPER,
            DruidStorageHandlerUtils.SMILE_MAPPER, DruidStorageHandler.getHttpClient()
    );
  }

  protected abstract JavaType getResultTypeDef();

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public DruidWritable createValue() {
    return new DruidWritable(false);
  }

  @Override
  public abstract boolean next(NullWritable key, DruidWritable value) throws IOException;

  @Override
  public long getPos() {
    // HiveContextAwareRecordReader uses this position to track the block position and check
    // whether to skip header and footer. return -1 to since we need not skip any header and
    // footer rows for druid.
    return -1;
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
    CloseQuietly.close(queryResultsIterator);
  }

  /**
   * This is a helper wrapper class used to create an iterator of druid rows out of InputStream.
   * The type of the rows is defined by org.apache.hadoop.hive.druid.serde.DruidQueryRecordReader.JsonParserIterator#typeRef
   *
   * @param <R> druid Row type returned as result
   */
  protected class JsonParserIterator<R extends Comparable<R>> implements Iterator<R>, Closeable
  {
    private JsonParser jp;
    private ObjectCodec objectCodec;
    private final ObjectMapper mapper;
    private final JavaType typeRef;
    private final Future<InputStream> future;
    private final Query query;
    private final String url;

    /**
     * @param mapper mapper used to deserialize the stream of data (we use smile factory)
     * @param typeRef Type definition of the results objects
     * @param future Future holding the input stream (the input stream is not owned but it will be closed when org.apache.hadoop.hive.druid.serde.DruidQueryRecordReader.JsonParserIterator#close() is called or reach the end of the steam)
     * @param url URL used to fetch the data, used mostly as message with exception stack to identify the faulty stream, thus this can be empty string.
     * @param query Query used to fetch the data, used mostly as message with exception stack, thus can be empty string.
     */
    public JsonParserIterator(ObjectMapper mapper,
            JavaType typeRef,
            Future<InputStream> future,
            String url,
            Query query
    )
    {
      this.typeRef = typeRef;
      this.future = future;
      this.url = url;
      this.query = query;
      this.mapper = mapper;
      jp = null;
    }

    @Override
    public boolean hasNext()
    {
      init();

      if (jp.isClosed()) {
        return false;
      }
      if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
        CloseQuietly.close(jp);
        return false;
      }

      return true;
    }

    @Override
    public R next()
    {
      init();

      try {
        final R retVal = objectCodec.readValue(jp, typeRef);
        jp.nextToken();
        return retVal;
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    private void init()
    {
      if (jp == null) {
        try {
          InputStream is = future.get();
          if (is == null) {
            throw  new IOException(String.format("query[%s] url[%s] timed out", query, url));
          } else {
            jp = mapper.getFactory().createParser(is).configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
          }
          final JsonToken nextToken = jp.nextToken();
          if (nextToken == JsonToken.START_OBJECT) {
            QueryInterruptedException cause = jp.getCodec().readValue(jp, QueryInterruptedException.class);
            throw new QueryInterruptedException(cause);
          } else if (nextToken != JsonToken.START_ARRAY) {
            throw new IAE("Next token wasn't a START_ARRAY, was[%s] from url [%s]", jp.getCurrentToken(), url);
          } else {
            jp.nextToken();
            objectCodec = jp.getCodec();
          }
        }
        catch (IOException | InterruptedException | ExecutionException e) {
          throw new RE(
                  e,
                  "Failure getting results for query[%s] url[%s] because of [%s]",
                  query,
                  url,
                  e.getMessage()
          );
        }
      }
    }

    @Override
    public void close() throws IOException
    {
      CloseQuietly.close(jp);
    }
  }


}
