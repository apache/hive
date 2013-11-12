/**
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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * The interface for reading ORC files.
 *
 * One Reader can support multiple concurrent RecordReader.
 */
public interface Reader {

  /**
   * Get the number of rows in the file.
   * @return the number of rows
   */
  long getNumberOfRows();

  /**
   * Get the deserialized data size of the file
   * @return raw data size
   */
  long getRawDataSize();

  /**
   * Get the deserialized data size of the specified columns
   * @param colNames
   * @return raw data size of columns
   */
  long getRawDataSizeOfColumns(List<String> colNames);

  /**
   * Get the user metadata keys.
   * @return the set of metadata keys
   */
  Iterable<String> getMetadataKeys();

  /**
   * Get a user metadata value.
   * @param key a key given by the user
   * @return the bytes associated with the given key
   */
  ByteBuffer getMetadataValue(String key);

  /**
   * Get the compression kind.
   * @return the kind of compression in the file
   */
  CompressionKind getCompression();

  /**
   * Get the buffer size for the compression.
   * @return number of bytes to buffer for the compression codec.
   */
  int getCompressionSize();

  /**
   * Get the number of rows per a entry in the row index.
   * @return the number of rows per an entry in the row index or 0 if there
   * is no row index.
   */
  int getRowIndexStride();

  /**
   * Get the list of stripes.
   * @return the information about the stripes in order
   */
  Iterable<StripeInformation> getStripes();

  /**
   * Get the object inspector for looking at the objects.
   * @return an object inspector for each row returned
   */
  ObjectInspector getObjectInspector();

  /**
   * Get the length of the file.
   * @return the number of bytes in the file
   */
  long getContentLength();

  /**
   * Get the statistics about the columns in the file.
   * @return the information about the column
   */
  ColumnStatistics[] getStatistics();

  /**
   * Get the metadata information like stripe level column statistics etc.
   * @return the information about the column
   * @throws IOException
   */
  Metadata getMetadata() throws IOException;

  /**
   * Get the list of types contained in the file. The root type is the first
   * type in the list.
   * @return the list of flattened types
   */
  List<OrcProto.Type> getTypes();

  /**
   * FileMetaInfo - represents file metadata stored in footer and postscript sections of the file
   * that is useful for Reader implementation
   *
   */
  class FileMetaInfo{
    final String compressionType;
    final int bufferSize;
    final int metadataSize;
    final ByteBuffer footerBuffer;
    FileMetaInfo(String compressionType, int bufferSize, int metadataSize, ByteBuffer footerBuffer){
      this.compressionType = compressionType;
      this.bufferSize = bufferSize;
      this.metadataSize = metadataSize;
      this.footerBuffer = footerBuffer;
    }
  }

  /**
   * Get the metadata stored in footer and postscript sections of the file
   * @return MetaInfo object with file metadata
   */
  FileMetaInfo getFileMetaInfo();

  /**
   * Create a RecordReader that will scan the entire file.
   * @param include true for each column that should be included
   * @return A new RecordReader
   * @throws IOException
   */
  RecordReader rows(boolean[] include) throws IOException;

  /**
   * Create a RecordReader that will start reading at the first stripe after
   * offset up to the stripe that starts at offset + length. This is intended
   * to work with MapReduce's FileInputFormat where divisions are picked
   * blindly, but they must cover all of the rows.
   * @param offset a byte offset in the file
   * @param length a number of bytes in the file
   * @param include true for each column that should be included
   * @return a new RecordReader that will read the specified rows.
   * @throws IOException
   * @deprecated
   */
  @Deprecated
  RecordReader rows(long offset, long length,
                    boolean[] include) throws IOException;

  /**
   * Create a RecordReader that will read a section of a file. It starts reading
   * at the first stripe after the offset and continues to the stripe that
   * starts at offset + length. It also accepts a list of columns to read and a
   * search argument.
   * @param offset the minimum offset of the first stripe to read
   * @param length the distance from offset of the first address to stop reading
   *               at
   * @param include true for each column that should be included
   * @param sarg a search argument that limits the rows that should be read.
   * @param neededColumns the names of the included columns
   * @return the record reader for the rows
   */
  RecordReader rows(long offset, long length,
                    boolean[] include, SearchArgument sarg,
                    String[] neededColumns) throws IOException;

}
