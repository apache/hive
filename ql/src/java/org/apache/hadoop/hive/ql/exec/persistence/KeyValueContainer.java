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
package org.apache.hadoop.hive.ql.exec.persistence;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * An eager key/value container that puts every row directly to output stream.
 * Kryo is used for the serialization/deserialization.
 * When reading, we load IN_MEMORY_NUM_ROWS rows from input stream to memory batch by batch.
 */
@SuppressWarnings("unchecked")
public class KeyValueContainer {
  private static final Logger LOG = LoggerFactory.getLogger(KeyValueContainer.class);

  @VisibleForTesting
  static final int IN_MEMORY_NUM_ROWS = 1024;

  private ObjectPair<HiveKey, BytesWritable>[] readBuffer;
  private boolean readBufferUsed = false; // indicates if read buffer has data
  private int rowsInReadBuffer = 0;       // number of rows in the temporary read buffer
  private int readCursor = 0;             // cursor during reading
  private int rowsOnDisk = 0;             // total number of pairs in output

  private File parentDir;
  private File tmpFile;

  private Input input;
  private Output output;

  public KeyValueContainer(String spillLocalDirs) {
    readBuffer = new ObjectPair[IN_MEMORY_NUM_ROWS];
    for (int i = 0; i < IN_MEMORY_NUM_ROWS; i++) {
      readBuffer[i] = new ObjectPair<HiveKey, BytesWritable>();
    }
    try {
      setupOutput(spillLocalDirs);
    } catch (IOException | HiveException e) {
      throw new RuntimeException("Failed to create temporary output file on disk", e);
    }
  }

  private void setupOutput(String spillLocalDirs) throws IOException, HiveException {
    FileOutputStream fos = null;
    try {
      if (parentDir == null) {
        parentDir = FileUtils.createLocalDirsTempFile(spillLocalDirs, "key-value-container", "", true);
        parentDir.deleteOnExit();
      }

      if (tmpFile == null || input != null) {
        tmpFile = File.createTempFile("KeyValueContainer", ".tmp", parentDir);
        LOG.info("KeyValueContainer created temp file " + tmpFile.getAbsolutePath());
        tmpFile.deleteOnExit();
      }

      fos = new FileOutputStream(tmpFile);
      output = new Output(fos);
    } catch (IOException e) {
      throw new HiveException(e);
    } finally {
      if (output == null && fos != null) {
        fos.close();
      }
    }
  }

  private BytesWritable readValue(Input input) {
    return new BytesWritable(input.readBytes(input.readInt()));
  }

  private void writeValue(Output output, BytesWritable bytesWritable) {
    int size = bytesWritable.getLength();
    output.writeInt(size);
    output.writeBytes(bytesWritable.getBytes(), 0, size);
  }

  private HiveKey readHiveKey(Input input) {
    HiveKey hiveKey = new HiveKey(input.readBytes(input.readInt()), input.readInt());
    hiveKey.setDistKeyLength(input.readInt());
    return hiveKey;
  }

  private void writeHiveKey(Output output, HiveKey hiveKey) {
    int size = hiveKey.getLength();
    output.writeInt(size);
    output.writeBytes(hiveKey.getBytes(), 0, size);
    output.writeInt(0); // Since hashCode is not used, just put an arbitrary number
    output.writeInt(hiveKey.getDistKeyLength());
  }

  public void add(HiveKey key, BytesWritable value) {
    writeHiveKey(output, key);
    writeValue(output, value);
    rowsOnDisk++;
  }

  public void clear() {
    readCursor = rowsInReadBuffer = rowsOnDisk = 0;
    readBufferUsed = false;

    if (parentDir != null) {
      if (input != null) {
        try {
          input.close();
        } catch (Throwable ignored) {
        }
        input = null;
      }
      if (output != null) {
        try {
          output.close();
        } catch (Throwable ignored) {
        }
        output = null;
      }
      try {
        FileUtil.fullyDelete(parentDir);
      } catch (Throwable ignored) {
      }
      parentDir = null;
      tmpFile = null;
    }
  }

  public boolean hasNext() {
    return readBufferUsed || rowsOnDisk > 0;
  }

  public ObjectPair<HiveKey, BytesWritable> next() {
    Preconditions.checkState(hasNext());
    if (!readBufferUsed) {
      try {
        if (input == null && output != null) {
          // Close output stream if open
          output.close();
          output = null;

          FileInputStream fis = null;
          try {
            fis = new FileInputStream(tmpFile);
            input = new Input(fis);
          } finally {
            if (input == null && fis != null) {
              fis.close();
            }
          }
        }
        if (input != null) {
          // Load next batch from disk
          if (rowsOnDisk >= IN_MEMORY_NUM_ROWS) {
            rowsInReadBuffer = IN_MEMORY_NUM_ROWS;
          } else {
            rowsInReadBuffer = rowsOnDisk;
          }

          for (int i = 0; i < rowsInReadBuffer; i++) {
            ObjectPair<HiveKey, BytesWritable> pair = readBuffer[i];
            pair.setFirst(readHiveKey(input));
            pair.setSecond(readValue(input));
          }

          if (input.eof()) {
            input.close();
            input = null;
          }

          readBufferUsed = true;
          readCursor = 0;
          rowsOnDisk -= rowsInReadBuffer;
        }
      } catch (Exception e) {
        clear(); // Clean up the cache
        throw new RuntimeException("Failed to load key/value pairs from disk", e);
      }
    }

    ObjectPair<HiveKey, BytesWritable> row = readBuffer[readCursor];
    if (++readCursor >= rowsInReadBuffer) {
      readBufferUsed = false;
      rowsInReadBuffer = 0;
      readCursor = 0;
    }
    return row;
  }

  public int numRowsInReadBuffer() {
    return rowsInReadBuffer;
  }

  public int size() {
    return rowsInReadBuffer + rowsOnDisk;
  }
}
