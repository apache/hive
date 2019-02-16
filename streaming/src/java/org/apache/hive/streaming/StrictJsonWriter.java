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

import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.JsonSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;

/**
 * Streaming Writer handles utf8 encoded Json (Strict syntax).
 * Uses {@link JsonSerDe} to process Json input
 *
 * NOTE: This record writer is NOT thread-safe. Use one record writer per streaming connection.
 */
public class StrictJsonWriter extends AbstractRecordWriter {
  private JsonSerDe serde;

  public StrictJsonWriter(final Builder builder) {
    super(builder.lineDelimiter);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String lineDelimiter;

    public Builder withLineDelimiterPattern(final String lineDelimiter) {
      this.lineDelimiter = lineDelimiter;
      return this;
    }

    public StrictJsonWriter build() {
      return new StrictJsonWriter(this);
    }
  }

  /**
   * Creates JsonSerDe
   *
   * @throws SerializationError if serde could not be initialized
   */
  @Override
  public JsonSerDe createSerde() throws SerializationError {
    try {
      Properties tableProps = table.getMetadata();
      tableProps.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(",").join(inputColumns));
      tableProps.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(":").join(inputTypes));
      JsonSerDe serde = new JsonSerDe();
      SerDeUtils.initializeSerDe(serde, conf, tableProps, null);
      this.serde = serde;
      return serde;
    } catch (SerDeException e) {
      throw new SerializationError("Error initializing serde " + JsonSerDe.class.getName(), e);
    }
  }

  @Override
  public Object encode(byte[] utf8StrRecord) throws SerializationError {
    try {
      Text blob = new Text(utf8StrRecord);
      return serde.deserialize(blob);
    } catch (SerDeException e) {
      throw new SerializationError("Unable to convert byte[] record into Object", e);
    }
  }
}
