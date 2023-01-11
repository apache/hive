/*
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
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetFilterPredicateConverter;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
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
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ParquetRecordReaderBase {
  public static final Logger LOG = LoggerFactory.getLogger(ParquetRecordReaderBase.class);

  protected final FileSplit fileSplit;
  protected Path filePath;
  protected ParquetInputSplit parquetInputSplit;
  protected ParquetMetadata parquetMetadata;
  protected ProjectionPusher projectionPusher;
  protected boolean skipTimestampConversion = false;
  protected Boolean skipProlepticConversion;
  protected Boolean legacyConversionEnabled;
  protected SerDeStats serDeStats;
  protected final JobConf jobConf;

  protected int schemaSize;
  protected List<BlockMetaData> filteredBlocks;
  protected ParquetFileReader reader;

  protected ParquetRecordReaderBase(JobConf conf, InputSplit oldSplit) throws IOException {
    serDeStats = new SerDeStats();
    projectionPusher = new ProjectionPusher();

    if (!(oldSplit instanceof FileSplit)) {
      throw new IllegalArgumentException("Unknown split type: " + oldSplit);
    }
    this.fileSplit = (FileSplit) oldSplit;
    this.jobConf = projectionPusher.pushProjectionsAndFilters(conf, fileSplit.getPath().getParent());
    this.filePath = fileSplit.getPath();
  }

  protected void setupMetadataAndParquetSplit(JobConf conf, ParquetMetadata metadata) throws IOException {
    // In the case of stat tasks a dummy split is created with -1 length but real path...
    if (fileSplit.getLength() != 0) {
      this.parquetMetadata = metadata != null ? metadata : getParquetMetadata(filePath, conf);
      parquetInputSplit = getSplit(conf);
    }
    // having null as parquetInputSplit seems to be a valid case based on this file's history
  }

  /**
   * gets a ParquetInputSplit corresponding to a split given by Hive
   *
   * @param conf The JobConf of the Hive job
   * @return a ParquetInputSplit corresponding to the oldSplit
   * @throws IOException if the config cannot be enhanced or if the footer cannot be read from the file
   */
  @SuppressWarnings("deprecation")
  protected ParquetInputSplit getSplit(
    final JobConf conf
  ) throws IOException {

    ParquetInputSplit split;
    final Path finalPath = fileSplit.getPath();

    // TODO enable MetadataFilter by using readFooter(Configuration configuration, Path file,
    // MetadataFilter filter) API
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
    final List<BlockMetaData> splitGroup = new ArrayList<BlockMetaData>();
    final long splitStart = fileSplit.getStart();
    final long splitLength = fileSplit.getLength();
    for (final BlockMetaData block : blocks) {
      final long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
      if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
        splitGroup.add(block);
      }
    }
    if (splitGroup.isEmpty()) {
      LOG.warn("Skipping split, could not find row group in: " + fileSplit);
      return null;
    }

    FilterCompat.Filter filter = setFilter(jobConf, fileMetaData.getSchema());
    if (filter != null) {
      filteredBlocks = RowGroupFilter.filterRowGroups(filter, splitGroup, fileMetaData.getSchema());
      if (filteredBlocks.isEmpty()) {
        LOG.debug("All row groups are dropped due to filter predicates");
        return null;
      }

      long droppedBlocks = splitGroup.size() - filteredBlocks.size();
      if (droppedBlocks > 0) {
        LOG.debug("Dropping " + droppedBlocks + " row groups that do not pass filter predicate");
      }
    } else {
      filteredBlocks = splitGroup;
    }

    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
      skipTimestampConversion = !Strings.nullToEmpty(fileMetaData.getCreatedBy()).startsWith("parquet-mr");
    }
    skipProlepticConversion = DataWritableReadSupport
        .getWriterDateProleptic(fileMetaData.getKeyValueMetaData());
    if (skipProlepticConversion == null) {
      skipProlepticConversion = HiveConf.getBoolVar(
          conf, HiveConf.ConfVars.HIVE_PARQUET_DATE_PROLEPTIC_GREGORIAN_DEFAULT);
    }
      legacyConversionEnabled =
          DataWritableReadSupport.getZoneConversionLegacy(fileMetaData.getKeyValueMetaData(), conf);

    split = new ParquetInputSplit(finalPath,
      splitStart,
      splitLength,
      fileSplit.getLocations(),
      filteredBlocks,
      readContext.getRequestedSchema().toString(),
      fileMetaData.getSchema().toString(),
      fileMetaData.getKeyValueMetaData(),
      readContext.getReadSupportMetadata());
    return split;
  }

  @SuppressWarnings("deprecation")
  protected ParquetMetadata getParquetMetadata(Path path, JobConf conf) throws IOException {
    return ParquetFileReader.readFooter(jobConf, path);
  }

  public FilterCompat.Filter setFilter(final JobConf conf, MessageType schema) {
    SearchArgument sarg = ConvertAstToSearchArg.createFromConf(conf);
    if (sarg == null) {
      return null;
    }

    String columnTypes = conf.get(IOConstants.COLUMNS_TYPES);
    String columnNames = conf.get(IOConstants.COLUMNS);
    List<TypeInfo> columnTypeList = TypeInfoUtils.getTypeInfosFromTypeString(columnTypes);
    Map<String, TypeInfo> columns = new HashMap<>();
    String[] names = columnNames.split(",");
    for (int i = 0; i < names.length; i++) {
      columns.put(names[i], columnTypeList.get(i));
    }

    // Create the Parquet FilterPredicate without including columns that do not exist
    // on the schema (such as partition columns).
    MessageType newSchema = getSchemaWithoutPartitionColumns(conf, schema);
    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, newSchema, columns);
    if (p != null) {
      // Filter may have sensitive information. Do not send to debug.
      LOG.debug("PARQUET predicate push down generated.");
      ParquetInputFormat.setFilterPredicate(conf, p);
      return FilterCompat.get(p);
    } else {
      // Filter may have sensitive information. Do not send to debug.
      LOG.debug("No PARQUET predicate push down is generated.");
      return null;
    }
  }

  private MessageType getSchemaWithoutPartitionColumns(JobConf conf, MessageType schema) {
    List<String> partCols = Utilities.getPartitionColumnNames(conf);
    if (partCols.isEmpty()) {
      return schema;
    }
    List<Type> newFields = new ArrayList<>();
    for (Type field : schema.getFields()) {
      if (!partCols.contains(field.getName())) {
        newFields.add(field);
      }
    }
    return new MessageType(schema.getName(), newFields);
  }

  public List<BlockMetaData> getFilteredBlocks() {
    return filteredBlocks;
  }

  public SerDeStats getStats() {
    return serDeStats;
  }
}
