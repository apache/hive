/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.iceberg.org.apache.orc.TypeDescription;
import org.apache.hive.iceberg.org.apache.orc.impl.OrcTail;
import org.apache.hive.iceberg.org.apache.orc.impl.ReaderImpl;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.mapping.MappingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities that rely on Iceberg code from org.apache.iceberg.orc package and are required for ORC vectorization.
 */
public class VectorizedReadUtils {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedReadUtils.class);

  private VectorizedReadUtils() {

  }

  /**
   * Opens the ORC inputFile and reads the metadata information to construct a byte buffer with OrcTail content.
   * Note that org.apache.orc (aka Hive bundled) ORC is used, as it is the older version compared to Iceberg's ORC.
   * @param path - the original ORC file - this needs to be accessed to retrieve the original schema for mapping
   * @param job - JobConf instance to adjust
   * @param fileId - FileID for the input file, serves as cache key in an LLAP setup
   * @throws IOException - errors relating to accessing the ORC file
   */
  public static ByteBuffer getSerializedOrcTail(Path path, SyntheticFileId fileId, JobConf job)
      throws IOException {

    ByteBuffer result = null;

    if (HiveConf.getBoolVar(job, HiveConf.ConfVars.LLAP_IO_ENABLED, LlapProxy.isDaemon()) &&
        LlapProxy.getIo() != null) {
      MapWork mapWork = LlapHiveUtils.findMapWork(job);
      PartitionDesc partitionDesc = LlapHiveUtils.partitionDescForPath(path, mapWork.getPathToPartitionInfo());

      // Note: Since Hive doesn't know about partition information of Iceberg tables, partitionDesc is only used to
      // deduct the table (and DB) name here.
      CacheTag cacheTag = HiveConf.getBoolVar(job, HiveConf.ConfVars.LLAP_TRACK_CACHE_USAGE) ?
          LlapHiveUtils.getDbAndTableNameForMetrics(path, true, partitionDesc) : null;

      try {
        // Schema has to be serialized and deserialized as it is passed between different packages of TypeDescription:
        // Iceberg expects org.apache.hive.iceberg.org.apache.orc.TypeDescription as it shades ORC, while LLAP provides
        // the unshaded org.apache.orc.TypeDescription type.
        return LlapProxy.getIo().getOrcTailFromCache(path, job, cacheTag, fileId).getSerializedTail();
      } catch (IOException ioe) {
        LOG.warn("LLAP is turned on but was unable to get file metadata information through its cache for {}",
            path, ioe);
      }

    }

    // Fallback to simple ORC reader file opening method in lack of or failure of LLAP.
    if (result == null) {
      org.apache.orc.OrcFile.ReaderOptions readerOptions =
          org.apache.orc.OrcFile.readerOptions(job).useUTCTimestamp(true).filesystem(path.getFileSystem(job));

      try (org.apache.orc.impl.ReaderImpl orcFileReader =
               (org.apache.orc.impl.ReaderImpl) OrcFile.createReader(path, readerOptions)) {
        return orcFileReader.getSerializedFileFooter();
      }
    }

    return result;
  }

  /**
   * Returns an unshaded version of the OrcTail of the supplied input file. Used by Hive classes.
   * @param serializedTail - ByteBuffer containing the tail bytes
   * @throws IOException - errors relating to deserialization
   */
  public static org.apache.orc.impl.OrcTail deserializeToOrcTail(ByteBuffer serializedTail) throws IOException {
    return org.apache.orc.impl.ReaderImpl.extractFileTail(serializedTail);
  }

  /**
   * Returns an Iceberg-shaded version of the OrcTail of the supplied input file. Used by Iceberg classes.
   * @param serializedTail - ByteBuffer containing the tail bytes
   * @throws IOException - errors relating to deserialization
   */
  public static OrcTail deserializeToShadedOrcTail(ByteBuffer serializedTail) throws IOException {
    return ReaderImpl.extractFileTail(serializedTail);
  }

  /**
   * Adjusts the jobConf so that column reorders and renames that might have happened since this ORC file was written
   * are properly mapped to the schema of the original file.
   * @param task - Iceberg task - required for
   * @param job - JobConf instance to adjust
   * @param fileSchema - ORC file schema of the input file
   * @throws IOException - errors relating to accessing the ORC file
   */
  public static void handleIcebergProjection(FileScanTask task, JobConf job,
      TypeDescription fileSchema, Expression residual)
      throws IOException {

    // We need to map with the current (i.e. current Hive table columns) full schema (without projections),
    // as OrcInputFormat will take care of the projections by the use of an include boolean array
    PartitionSpec spec = task.spec();
    Schema currentSchema = spec.schema();

    TypeDescription readOrcSchema;
    if (ORCSchemaUtil.hasIds(fileSchema)) {
      readOrcSchema = ORCSchemaUtil.buildOrcProjection(currentSchema, fileSchema);
    } else {
      Schema readSchemaForOriginalFile = currentSchema;
      // In case of migrated, originally partitioned tables, partition values are not present in the file
      if (spec.isPartitioned()) {
        readSchemaForOriginalFile = currentSchema.select(currentSchema.columns().stream()
            .filter(c -> !spec.identitySourceIds().contains(c.fieldId()))
            .map(c -> c.name())
            .collect(Collectors.toList()));
      }

      TypeDescription typeWithIds =
          ORCSchemaUtil.applyNameMapping(fileSchema, MappingUtil.create(currentSchema));
      readOrcSchema = ORCSchemaUtil.buildOrcProjection(readSchemaForOriginalFile, typeWithIds);
    }

    job.set(ColumnProjectionUtils.ORC_SCHEMA_STRING, readOrcSchema.toString());

    // Predicate pushdowns needs to be adjusted too in case of column renames, we let Iceberg generate this into job
    Expression boundFilter = Binder.bind(currentSchema.asStruct(), residual, false);

    // Note the use of the unshaded version of this class here (required for SARG deseralization later)
    org.apache.hadoop.hive.ql.io.sarg.SearchArgument sarg =
        ExpressionToOrcSearchArgument.convert(boundFilter, readOrcSchema);
    if (sarg != null) {
      job.unset(TableScanDesc.FILTER_EXPR_CONF_STR);
      job.unset(ConvertAstToSearchArg.SARG_PUSHDOWN);

      job.set(ConvertAstToSearchArg.SARG_PUSHDOWN, ConvertAstToSearchArg.sargToKryo(sarg));
    }
  }
}
