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
package org.apache.hadoop.hive.ql.io.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetTableUtils;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.ql.io.parquet.write.ParquetRecordWriterWrapper;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.hadoop.ParquetOutputFormat;

/**
 *
 * A Parquet OutputFormat for Hive (with the deprecated package mapred)
 *
 */
public class MapredParquetOutputFormat extends FileOutputFormat<NullWritable, ParquetHiveRecord>
    implements HiveOutputFormat<NullWritable, ParquetHiveRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(MapredParquetOutputFormat.class);

  protected ParquetOutputFormat<ParquetHiveRecord> realOutputFormat;

  public MapredParquetOutputFormat() {
    realOutputFormat = new ParquetOutputFormat<ParquetHiveRecord>(new DataWritableWriteSupport());
  }

  public MapredParquetOutputFormat(final OutputFormat<Void, ParquetHiveRecord> mapreduceOutputFormat) {
    realOutputFormat = (ParquetOutputFormat<ParquetHiveRecord>) mapreduceOutputFormat;
  }

  @Override
  public void checkOutputSpecs(final FileSystem ignored, final JobConf job) throws IOException {
    realOutputFormat.checkOutputSpecs(ShimLoader.getHadoopShims().getHCatShim().createJobContext(job, null));
  }

  @Override
  public RecordWriter<NullWritable, ParquetHiveRecord> getRecordWriter(
      final FileSystem ignored,
      final JobConf job,
      final String name,
      final Progressable progress
      ) throws IOException {
    throw new RuntimeException("Should never be used");
  }

  /**
   *
   * Create the parquet schema from the hive schema, and return the RecordWriterWrapper which
   * contains the real output format
   */
  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
      final JobConf jobConf,
      final Path finalOutPath,
      final Class<? extends Writable> valueClass,
      final boolean isCompressed,
      final Properties tableProperties,
      final Progressable progress) throws IOException {

    LOG.info("creating new record writer..." + this);

    final String columnNameProperty = tableProperties.getProperty(IOConstants.COLUMNS);
    final String columnTypeProperty = tableProperties.getProperty(IOConstants.COLUMNS_TYPES);
    List<String> columnNames;
    List<TypeInfo> columnTypes;
    final String columnNameDelimiter = tableProperties.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tableProperties
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }

    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    DataWritableWriteSupport.setSchema(HiveSchemaConverter.convert(columnNames, columnTypes), jobConf);
    DataWritableWriteSupport.setTimeZone(getParquetWriterTimeZone(tableProperties), jobConf);

    return getParquerRecordWriterWrapper(realOutputFormat, jobConf, finalOutPath.toString(),
            progress,tableProperties);
  }

  protected ParquetRecordWriterWrapper getParquerRecordWriterWrapper(
      ParquetOutputFormat<ParquetHiveRecord> realOutputFormat,
      JobConf jobConf,
      String finalOutPath,
      Progressable progress,
      Properties tableProperties
      ) throws IOException {
    return new ParquetRecordWriterWrapper(realOutputFormat, jobConf, finalOutPath.toString(),
            progress,tableProperties);
  }

  private TimeZone getParquetWriterTimeZone(Properties tableProperties) {
    // PARQUET_INT96_WRITE_ZONE_PROPERTY is a table property used to detect what timezone
    // conversion to use when writing Parquet timestamps.
    String timeZoneID =
        tableProperties.getProperty(ParquetTableUtils.PARQUET_INT96_WRITE_ZONE_PROPERTY);
    if (!Strings.isNullOrEmpty(timeZoneID)) {

      NanoTimeUtils.validateTimeZone(timeZoneID);
      return TimeZone.getTimeZone(timeZoneID);
    }

    return TimeZone.getDefault();
  }
}
