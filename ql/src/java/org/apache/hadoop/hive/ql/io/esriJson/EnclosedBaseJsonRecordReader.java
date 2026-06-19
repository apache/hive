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
package org.apache.hadoop.hive.ql.io.esriJson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Enumerates records from an Enclosed JSON file - use either Esri JSON or GeoJSON subclass
 */
public abstract class EnclosedBaseJsonRecordReader extends RecordReader<LongWritable, Text>
    implements org.apache.hadoop.mapred.RecordReader<LongWritable, Text> {
  static final Logger LOG = LoggerFactory.getLogger(EnclosedBaseJsonRecordReader.class.getName());

  protected LongWritable mkey = null;
  protected Text mval = null;
  protected InputStream inputStream;
  protected long splitLen = 0;  // for getProgress
  protected JsonParser parser;

  protected EnclosedBaseJsonRecordReader() throws IOException {
    mkey = createKey();
    mval = createValue();
  }

  protected EnclosedBaseJsonRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration conf)
      throws IOException {
    org.apache.hadoop.mapred.FileSplit fileSplit = (org.apache.hadoop.mapred.FileSplit) split;
    splitLen = fileSplit.getLength();  // using MRv1
    commonInit(fileSplit.getPath(), conf);
  }

  @Override
  public void close() throws IOException {
    if (inputStream != null)
      inputStream.close();
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public Text createValue() {
    return new Text();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return mkey;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return mval;
  }

  @Override
  public long getPos() throws IOException {
    if (parser == null) {
      return 0;
    } else {
      return parser.getCurrentLocation().getCharOffset();
    }
  }

  @Override
  public float getProgress() throws IOException {
    if (splitLen == 0 || parser == null)
      return 0;

    return (float) parser.getCurrentLocation().getByteOffset() / splitLen;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext taskContext) throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    splitLen = fileSplit.getLength();  // using MRv2
    commonInit(fileSplit.getPath(), taskContext.getConfiguration());
  }

  // Both Esri JSON and GeoJSON conveniently have "features"
  @Override
  public boolean next(LongWritable key, Text value) throws IOException {
    JsonToken token;

    // first call to nextKeyValue() so we need to create the parser and move to the
    // feature array
    if (parser == null) {
      parser = new JsonFactory().createJsonParser(inputStream);

      parser.setCodec(new ObjectMapper());

      token = parser.nextToken();

      while (token != null && !(token == JsonToken.START_ARRAY && parser.getCurrentName() != null && parser
          .getCurrentName().equals("features"))) {
        token = parser.nextToken();
      }

      if (token == null)
        return false; // never found the features array
    }

    key.set(parser.getCurrentLocation().getCharOffset());

    token = parser.nextToken();

    // this token should be a start object with no name
    if (token == null || !(token == JsonToken.START_OBJECT && parser.getCurrentName() == null))
      return false;

    ObjectNode node = parser.readValueAsTree();

    value.set(node.toString());

    return true;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return next(mkey, mval);
  }

  private void commonInit(Path filePath, Configuration conf) throws IOException {
    FileSystem fs = filePath.getFileSystem(conf);
    inputStream = fs.open(filePath);
  }
}
