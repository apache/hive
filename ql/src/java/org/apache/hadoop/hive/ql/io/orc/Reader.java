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

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * The interface for reading ORC files.
 *
 * One Reader can support multiple concurrent RecordReader.
 */
public interface Reader extends org.apache.orc.Reader {

  /**
   * Get the object inspector for looking at the objects.
   * @return an object inspector for each row returned
   */
  ObjectInspector getObjectInspector();

  /**
   * Get the Compression kind in the compatibility mode.
   */
  CompressionKind getCompression();

  /**
   * Create a RecordReader that reads everything with the default options.
   * @return a new RecordReader
   * @throws IOException
   */
  RecordReader rows() throws IOException;

  /**
   * Create a RecordReader that reads everything with the given options.
   * @param options the options to use
   * @return a new RecordReader
   * @throws IOException
   */
  RecordReader rowsOptions(Options options) throws IOException;
  
  /**
   * Create a RecordReader that will scan the entire file.
   * This is a legacy method and rowsOptions is preferred.
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
   * This is a legacy method and rowsOptions is preferred.
   * @param offset a byte offset in the file
   * @param length a number of bytes in the file
   * @param include true for each column that should be included
   * @return a new RecordReader that will read the specified rows.
   * @throws IOException
   */
  RecordReader rows(long offset, long length,
                    boolean[] include) throws IOException;

  /**
   * Create a RecordReader that will read a section of a file. It starts reading
   * at the first stripe after the offset and continues to the stripe that
   * starts at offset + length. It also accepts a list of columns to read and a
   * search argument.
   * This is a legacy method and rowsOptions is preferred.
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
