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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * Streaming Writer handles text input data with regex. Uses
 * org.apache.hadoop.hive.serde2.RegexSerDe
 *
 * NOTE: This record writer is NOT thread-safe. Use one record writer per streaming connection.
 */
public class StrictRegexWriter extends AbstractRecordWriter {
  private String regex;
  private RegexSerDe serde;

  private StrictRegexWriter(final Builder builder) {
    super(builder.lineDelimiter);
    this.regex = builder.regex;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String regex;
    private String lineDelimiter;

    public Builder withRegex(final String regex) {
      this.regex = regex;
      return this;
    }

    public Builder withLineDelimiterPattern(final String lineDelimiter) {
      this.lineDelimiter = lineDelimiter;
      return this;
    }

    public StrictRegexWriter build() {
      return new StrictRegexWriter(this);
    }
  }

  /**
   * Creates RegexSerDe
   *
   * @throws SerializationError if serde could not be initialized
   */
  @Override
  public RegexSerDe createSerde() throws SerializationError {
    try {
      Properties tableProps = table.getMetadata();
      tableProps.setProperty(RegexSerDe.INPUT_REGEX, regex);
      tableProps.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(",").join(inputColumns));
      tableProps.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(":").join(inputTypes));
      final String columnComments = tableProps.getProperty("columns.comments");
      if (columnComments != null) {
        List<String> comments = Lists.newArrayList(Splitter.on('\0').split(columnComments));
        int commentsSize = comments.size();
        for (int i = 0; i < inputColumns.size() - commentsSize; i++) {
          comments.add("");
        }
        tableProps.setProperty("columns.comments", Joiner.on('\0').join(comments));
      }
      RegexSerDe serde = new RegexSerDe();
      serde.initialize(conf, tableProps, null);
      this.serde = serde;
      return serde;
    } catch (SerDeException e) {
      throw new SerializationError("Error initializing serde " + RegexSerDe.class.getName(), e);
    }
  }

  /**
   * Encode Utf8 encoded string bytes using RegexSerDe
   *
   * @param utf8StrRecord - serialized record
   * @return The encoded object
   * @throws SerializationError - in case of any deserialization error
   */
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
