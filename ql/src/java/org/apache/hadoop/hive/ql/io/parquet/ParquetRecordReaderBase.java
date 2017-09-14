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

import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetFilterPredicateConverter;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.Preconditions;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ParquetRecordReaderBase {
  public static final Logger LOG = LoggerFactory.getLogger(ParquetRecordReaderBase.class);

  protected Path file;
  protected ProjectionPusher projectionPusher;
  protected boolean skipTimestampConversion = false;
  protected SerDeStats serDeStats;
  protected JobConf jobConf;

  protected int schemaSize;
  protected ParquetFileReader reader;

  /**
   * gets a ParquetInputSplit corresponding to a split given by Hive
   *
   * @param oldSplit The split given by Hive
   * @param conf The JobConf of the Hive job
   * @return a ParquetInputSplit corresponding to the oldSplit
   * @throws IOException if the config cannot be enhanced or if the footer cannot be read from the file
   */
  protected ParquetInputSplit getSplit(
    final org.apache.hadoop.mapred.InputSplit oldSplit,
    final JobConf conf
  ) throws IOException {
    Preconditions.checkArgument((oldSplit instanceof FileSplit), "Unknown split type:" + oldSplit);
    final Path finalPath = ((FileSplit) oldSplit).getPath();
    jobConf = projectionPusher.pushProjectionsAndFilters(conf, finalPath.getParent());

    // TODO enable MetadataFilter
    final ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(jobConf,
      finalPath, ParquetMetadataConverter.NO_FILTER);
    final List<BlockMetaData> blocks = parquetMetadata.getBlocks();
    final FileMetaData fileMetaData = parquetMetadata.getFileMetaData();

    final ReadSupport.ReadContext
      readContext = new DataWritableReadSupport().init(new InitContext(jobConf,
      null, fileMetaData.getSchema()));

    // Compute stats
    for (BlockMetaData bmd : blocks) {
      serDeStats.setRowCount(serDeStats.getRowCount() + bmd.getRowCount());
      serDeStats.setRawDataSize(serDeStats.getRawDataSize() + bmd.getTotalByteSize());
    }

    schemaSize = MessageTypeParser.parseMessageType(readContext.getReadSupportMetadata()
      .get(DataWritableReadSupport.HIVE_TABLE_AS_PARQUET_SCHEMA)).getFieldCount();

    final long splitStart = ((FileSplit) oldSplit).getStart();
    final long splitLength = ((FileSplit) oldSplit).getLength();

    setFilter(jobConf, fileMetaData.getSchema());

    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
      skipTimestampConversion = !Strings.nullToEmpty(fileMetaData.getCreatedBy()).startsWith("parquet-mr");
    }

    // rowGroupOffsets need to be set to null otherwise filter will not be applied
    // in ParquetRecordReader#initializeInternalReader
    return new ParquetInputSplit(finalPath,
      splitStart,
      splitStart + splitLength,
      splitLength,
      oldSplit.getLocations(),
      null);

  }

  private void setFilter(final JobConf conf, MessageType schema) {
    SearchArgument sarg = ConvertAstToSearchArg.createFromConf(conf);
    if (sarg == null) {
      return;
    }

    // Create the Parquet FilterPredicate without including columns that do not exist
    // on the schema (such as partition columns).
    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);
    if (p != null) {
      // Filter may have sensitive information. Do not send to debug.
      LOG.debug("PARQUET predicate push down generated.");
      ParquetInputFormat.setFilterPredicate(conf, p);
    } else {
      // Filter may have sensitive information. Do not send to debug.
      LOG.debug("No PARQUET predicate push down is generated.");
    }
  }

  public SerDeStats getStats() {
    return serDeStats;
  }
}
