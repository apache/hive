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
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;

import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 *
 * DataWritableWriteSupport is a WriteSupport for the DataWritableWriter
 *
 */
public class DataWritableWriteSupport extends WriteSupport<ParquetHiveRecord> {

  public static final String PARQUET_HIVE_SCHEMA = "parquet.hive.schema";
  private static final String PARQUET_TIMEZONE_CONVERSION = "parquet.hive.timezone";

  private DataWritableWriter writer;
  private MessageType schema;
  private TimeZone timeZone;

  public static void setSchema(final MessageType schema, final Configuration configuration) {
    configuration.set(PARQUET_HIVE_SCHEMA, schema.toString());
  }

  public static MessageType getSchema(final Configuration configuration) {
    return MessageTypeParser.parseMessageType(configuration.get(PARQUET_HIVE_SCHEMA));
  }

  public static void setTimeZone(final TimeZone timeZone, final Configuration configuration) {
    configuration.set(PARQUET_TIMEZONE_CONVERSION, timeZone.getID());
  }

  public static TimeZone getTimeZone(final Configuration configuration) {
    return TimeZone.getTimeZone(configuration.get(PARQUET_TIMEZONE_CONVERSION));
  }

  @Override
  public WriteContext init(final Configuration configuration) {
    schema = getSchema(configuration);
    timeZone = getTimeZone(configuration);
    return new WriteContext(schema, new HashMap<String, String>());
  }

  @Override
  public void prepareForWrite(final RecordConsumer recordConsumer) {
    writer = new DataWritableWriter(recordConsumer, schema, timeZone);
  }

  @Override
  public void write(final ParquetHiveRecord record) {
    writer.write(record);
  }
}
