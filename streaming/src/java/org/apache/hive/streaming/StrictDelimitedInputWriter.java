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

package org.apache.hive.streaming;


import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.BytesWritable;

import com.google.common.base.Joiner;

/**
 * Streaming Writer handles delimited input (eg. CSV).
 * Delimited input is parsed to extract partition values, bucketing info and is forwarded to record updater.
 * Uses Lazy Simple SerDe to process delimited input.
 *
 * NOTE: This record writer is NOT thread-safe. Use one record writer per streaming connection.
 */
public class StrictDelimitedInputWriter extends AbstractRecordWriter {
  private char fieldDelimiter;
  private char collectionDelimiter;
  private char mapKeyDelimiter;
  private LazySimpleSerDe serde;

  private StrictDelimitedInputWriter(Builder builder) {
    super(builder.lineDelimiter);
    this.fieldDelimiter = builder.fieldDelimiter;
    this.collectionDelimiter = builder.collectionDelimiter;
    this.mapKeyDelimiter = builder.mapKeyDelimiter;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private char fieldDelimiter = (char) LazySerDeParameters.DefaultSeparators[0];
    private char collectionDelimiter = (char) LazySerDeParameters.DefaultSeparators[1];
    private char mapKeyDelimiter = (char) LazySerDeParameters.DefaultSeparators[2];
    private String lineDelimiter;

    public Builder withFieldDelimiter(final char fieldDelimiter) {
      this.fieldDelimiter = fieldDelimiter;
      return this;
    }

    public Builder withCollectionDelimiter(final char collectionDelimiter) {
      this.collectionDelimiter = collectionDelimiter;
      return this;
    }

    public Builder withMapKeyDelimiter(final char mapKeyDelimiter) {
      this.mapKeyDelimiter = mapKeyDelimiter;
      return this;
    }

    public Builder withLineDelimiterPattern(final String lineDelimiter) {
      this.lineDelimiter = lineDelimiter;
      return this;
    }

    public StrictDelimitedInputWriter build() {
      return new StrictDelimitedInputWriter(this);
    }
  }

  @Override
  public Object encode(byte[] record) throws SerializationError {
    try {
      BytesWritable blob = new BytesWritable();
      blob.set(record, 0, record.length);
      return serde.deserialize(blob);
    } catch (SerDeException e) {
      throw new SerializationError("Unable to convert byte[] record into Object", e);
    }
  }

  @Override
  public LazySimpleSerDe createSerde() throws SerializationError {
    try {
      Properties tableProps = table.getMetadata();
      tableProps.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(",").join(inputColumns));
      tableProps.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(":").join(inputTypes));
      tableProps.setProperty(serdeConstants.FIELD_DELIM, String.valueOf(fieldDelimiter));
      tableProps.setProperty(serdeConstants.COLLECTION_DELIM, String.valueOf(collectionDelimiter));
      tableProps.setProperty(serdeConstants.MAPKEY_DELIM, String.valueOf(mapKeyDelimiter));
      LazySimpleSerDe serde = new LazySimpleSerDe();
      serde.initialize(conf, tableProps, null);
      this.serde = serde;
      return serde;
    } catch (SerDeException e) {
      throw new SerializationError("Error initializing serde", e);
    }
  }
}
