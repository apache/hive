/*
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * An eager object container that puts every row directly to output stream.
 * The object can be of any type.
 * Kryo is used for the serialization/deserialization.
 * When reading, we load IN_MEMORY_NUM_ROWS rows from input stream to memory batch by batch.
 */
@SuppressWarnings("unchecked")
public class ObjectContainer<ROW> {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectContainer.class);

  @VisibleForTesting
  static final int IN_MEMORY_NUM_ROWS = 1024;

  private ROW[] readBuffer;
  private boolean readBufferUsed = false; // indicates if read buffer has data
  private int rowsInReadBuffer = 0;       // number of rows in the temporary read buffer
  private int readCursor = 0;             // cursor during reading
  private int rowsOnDisk = 0;             // total number of objects in output

  private File parentDir;
  private File tmpFile;

  private Input input;
  private Output output;

  public ObjectContainer(String spillLocalDirs) {
    readBuffer = (ROW[]) new Object[IN_MEMORY_NUM_ROWS];
    for (int i = 0; i < IN_MEMORY_NUM_ROWS; i++) {
      readBuffer[i] = (ROW) new Object();
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
        parentDir = FileUtils.createLocalDirsTempFile(spillLocalDirs, "object-container", "", true);
      }

      if (tmpFile == null || input != null) {
        tmpFile = File.createTempFile("ObjectContainer", ".tmp", parentDir);
        LOG.info("ObjectContainer created temp file " + tmpFile.getAbsolutePath());
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

  public void add(ROW row) {
    Kryo kryo = SerializationUtilities.borrowKryo();
    try {
      kryo.writeClassAndObject(output, row);
    } finally {
      SerializationUtilities.releaseKryo(kryo);
    }
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

  public ROW next() {
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

          Kryo kryo = SerializationUtilities.borrowKryo();
          try {
            for (int i = 0; i < rowsInReadBuffer; i++) {
              readBuffer[i] = (ROW) kryo.readClassAndObject(input);
            }
          } finally {
            SerializationUtilities.releaseKryo(kryo);
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
        throw new RuntimeException("Failed to load rows from disk", e);
      }
    }

    ROW row = readBuffer[readCursor];
    if (++readCursor >= rowsInReadBuffer) {
      readBufferUsed = false;
      rowsInReadBuffer = 0;
      readCursor = 0;
    }
    return row;
  }

  public int size() {
    return rowsInReadBuffer + rowsOnDisk;
  }
}
