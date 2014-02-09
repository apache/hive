/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.write;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;

import parquet.hadoop.api.WriteSupport;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 *
 * DataWritableWriteSupport is a WriteSupport for the DataWritableWriter
 *
 */
public class DataWritableWriteSupport extends WriteSupport<ArrayWritable> {

  public static final String PARQUET_HIVE_SCHEMA = "parquet.hive.schema";

  private DataWritableWriter writer;
  private MessageType schema;

  public static void setSchema(final MessageType schema, final Configuration configuration) {
    configuration.set(PARQUET_HIVE_SCHEMA, schema.toString());
  }

  public static MessageType getSchema(final Configuration configuration) {
    return MessageTypeParser.parseMessageType(configuration.get(PARQUET_HIVE_SCHEMA));
  }

  @Override
  public WriteContext init(final Configuration configuration) {
    schema = getSchema(configuration);
    return new WriteContext(schema, new HashMap<String, String>());
  }

  @Override
  public void prepareForWrite(final RecordConsumer recordConsumer) {
    writer = new DataWritableWriter(recordConsumer, schema);
  }

  @Override
  public void write(final ArrayWritable record) {
    writer.write(record);
  }
}
