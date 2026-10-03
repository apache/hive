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

package org.apache.iceberg.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetValueReaders.PrimitiveReader;
import org.apache.iceberg.parquet.ParquetValueReaders.UnboxedReader;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

/**
 * Decodes a whole position delete file into an index per referenced data file. The spec requires the rows to be sorted
 * by file_path then pos, so the index is only looked up when the path changes; a path that comes back is merged into
 * its index. Parquet files are read straight from the path and pos column readers, without a record or a String per
 * row.
 */
final class PositionDeletesReader {
  private static final Schema POS_DELETE_SCHEMA = DeleteSchemaUtil.pathPosSchema();

  private PositionDeletesReader() {
  }

  static CharSequenceMap<PositionDeleteIndex> read(DeleteFile deleteFile, InputFile file) {
    return deleteFile.format() == FileFormat.PARQUET ? readParquet(deleteFile, file) : readRecords(deleteFile, file);
  }

  private static CharSequenceMap<PositionDeleteIndex> readParquet(DeleteFile deleteFile, InputFile file) {
    CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();
    try (CloseableIterable<PathPosReader> deletes = Parquet.read(file)
        .project(POS_DELETE_SCHEMA)
        .createReaderFunc(PathPosReader::new)
        .build()) {
      Binary lastPath = null;
      PositionDeleteIndex index = null;
      for (PathPosReader delete : deletes) {
        // Within a row group, a dictionary encoded path is the same instance on every row. Plain encoded paths, and
        // paths copied off a direct page buffer, fall back to equals.
        if (delete.path != lastPath && !delete.path.equals(lastPath)) {
          lastPath = delete.path.copy();
          index = indexes.computeIfAbsent(lastPath.toStringUsingUTF8(), path -> newIndex(path, deleteFile));
        }
        index.delete(delete.pos);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return indexes;
  }

  private static CharSequenceMap<PositionDeleteIndex> readRecords(DeleteFile deleteFile, InputFile file) {
    CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();
    try (CloseableIterable<Record> deletes = FormatModelRegistry.readBuilder(deleteFile.format(), Record.class, file)
        .project(POS_DELETE_SCHEMA)
        .reuseContainers()
        .build()) {
      CharSequence lastPath = null;
      PositionDeleteIndex index = null;
      for (Record delete : deletes) {
        CharSequence path = (CharSequence) delete.get(0);
        if (!path.equals(lastPath)) {
          lastPath = path.toString();
          index = indexes.computeIfAbsent(lastPath, key -> newIndex(key, deleteFile));
        }
        index.delete((Long) delete.get(1));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return indexes;
  }

  private static PositionDeleteIndex newIndex(CharSequence path, DeleteFile deleteFile) {
    return Deletes.toPositionIndex(path, CloseableIterable.<StructLike>empty(), deleteFile);
  }

  /**
   * Reads the path of a row as the bytes the column holds and its position as a primitive, into itself. It is only
   * ever the root reader, which Iceberg's ParquetReader drives through read and setPageSource alone, so it reports the
   * path column only.
   */
  private static final class PathPosReader extends PrimitiveReader<PathPosReader> {
    private final UnboxedReader<Long> posReader;
    private Binary path;
    private long pos;

    PathPosReader(MessageType fileSchema) {
      super(fileSchema.getColumnDescription(new String[] { MetadataColumns.DELETE_FILE_PATH.name() }));
      this.posReader = new UnboxedReader<>(
          fileSchema.getColumnDescription(new String[] { MetadataColumns.DELETE_FILE_POS.name() }));
    }

    @Override
    public PathPosReader read(PathPosReader reuse) {
      path = column.nextBinary();
      pos = posReader.readLong();
      return this;
    }

    @Override
    public void setPageSource(PageReadStore pages) {
      super.setPageSource(pages);
      posReader.setPageSource(pages);
    }
  }
}
