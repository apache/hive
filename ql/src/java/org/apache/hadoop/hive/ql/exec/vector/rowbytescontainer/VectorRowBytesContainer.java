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
package org.apache.hadoop.hive.ql.exec.vector.rowbytescontainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.serde2.ByteStream.Output;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * An eager bytes container that puts row bytes to an output stream.
 */
public class VectorRowBytesContainer {

  private static final Logger LOG = LoggerFactory.getLogger(VectorRowBytesContainer.class);

  private File parentDir;
  private File tmpFile;

  // We buffer in a org.apache.hadoop.hive.serde2.ByteStream.Output since that is what
  // is used by VectorSerializeRow / SerializeWrite.  Periodically, we flush this buffer
  // to disk.
  private Output output;
  private int rowBeginPos;
  private static final int OUTPUT_SIZE = 4096;
  private static final int THRESHOLD = 8 * (OUTPUT_SIZE / 10);
  private static final int INPUT_SIZE = 4096;

  private FileOutputStream fileOutputStream;

  private boolean isOpen;

  private byte[] readBuffer;
  private byte[] largeRowBuffer;
  private int readOffset;
  private int readLength;

  private int readNextCount;
  private int readNextIndex;

  private static final int MAX_READS = 256;
  private byte[][] readNextBytes;
  private int readNextOffsets[];
  private int readNextLengths[];

  private byte[] currentBytes;
  private int currentOffset;
  private int currentLength;

  private long totalWriteLength;
  private long totalReadLength;

  private FileInputStream fileInputStream;

  private final String spillLocalDirs;

  public VectorRowBytesContainer(String spillLocalDirs) {
    output = new Output();
    readBuffer = new byte[INPUT_SIZE];
    readNextBytes = new byte[MAX_READS][];
    readNextOffsets = new int[MAX_READS];
    readNextLengths = new int[MAX_READS];
    isOpen = false;
    totalWriteLength = 0;
    totalReadLength = 0;
    this.spillLocalDirs = spillLocalDirs;
  }

  private void setupOutputFileStreams() throws IOException {
    parentDir = FileUtils.createLocalDirsTempFile(spillLocalDirs, "bytes-container", "", true);
    parentDir.deleteOnExit();
    tmpFile = File.createTempFile("BytesContainer", ".tmp", parentDir);
    LOG.debug("BytesContainer created temp file " + tmpFile.getAbsolutePath());
    tmpFile.deleteOnExit();

    fileOutputStream = new FileOutputStream(tmpFile);
  }

  private void initFile() {
    try {
      setupOutputFileStreams();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temporary output file on disk", e);
    }
  }

  public Output getOutputForRowBytes() {
    if (!isOpen) {
      initFile();
      isOpen = true;
    }
    // Reserve space for the int length.
    output.reserve(4);
    rowBeginPos = output.getLength();
    return output;
  }

  public void finishRow() throws IOException {
    int length = output.getLength() - rowBeginPos;
    output.writeInt(rowBeginPos - 4, length);
    if (output.getLength() > THRESHOLD) {
      fileOutputStream.write(output.getData(), 0, output.getLength());
      totalWriteLength += output.getLength();
      output.reset();
    }
  }

  public void prepareForReading() throws IOException {
    if (!isOpen) {
      return;
    }
    if (output.getLength() > 0) {
      fileOutputStream.write(output.getData(), 0, output.getLength());
      totalWriteLength += output.getLength();
      fileOutputStream.flush();
      output.reset();
    }
    if (fileInputStream != null) {
      fileInputStream.close();
    }
    fileInputStream = new FileInputStream(tmpFile);
    readNextIndex = 0;
    readNextCount = 0;
  }

  private int readInt() {
      int value = (((readBuffer[readOffset] & 0xFF) << 24) |
                   ((readBuffer[readOffset + 1] & 0xFF) << 16) |
                   ((readBuffer[readOffset + 2] & 0xFF) << 8) |
                   ((readBuffer[readOffset + 3] & 0xFF)));
      readOffset += 4;
      return value;
  }

  // Call when nextReadIndex == nextReadCount.
  private void bufferedRead() throws IOException {

    // Reset for reading.
    readNextIndex = 0;

    // Reset for filling.
    readNextCount = 0;

    if (readOffset < readLength) {
      // Move unprocessed remainder to beginning of buffer.
      int unprocessLength = readLength - readOffset;
      System.arraycopy(readBuffer, readOffset, readBuffer, 0, unprocessLength);

      int maxReadLength = readBuffer.length - unprocessLength;
      int partialReadLength = fileInputStream.read(readBuffer, unprocessLength, maxReadLength);
      if (partialReadLength == -1) {
        partialReadLength = 0;
      }
      totalReadLength += partialReadLength;
      readLength = unprocessLength + partialReadLength;
      readOffset = 0;
    } else {
      readOffset = 0;
      readLength = fileInputStream.read(readBuffer, 0, readBuffer.length);
      if (readLength == -1) {
        readLength = 0;
      }
      totalReadLength += readLength;
    }
    if (readLength == 0) {
      return;
    }
    if (readLength < 0) {
      throw new IOException("Negative read length");
    }

    // Get length word.
    if (readLength < 4) {
      throw new IOException("Expecting 4 byte length");
    }

    while (true) {
      // Use Input class to read length.
      int saveReadOffset = readOffset;
      int rowLength = readInt();
      if (rowLength < 0) {
        throw new IOException("Negative row length");
      }
      int remainingLength = readLength - readOffset;
      if (remainingLength < rowLength) {
        if (readNextCount > 0) {
          // Leave this one for the next round.
          readOffset = saveReadOffset;
          break;
        }

        // Buffer needed to bridge.
        if (largeRowBuffer == null || largeRowBuffer.length < rowLength) {
          int newLargeBufferLength = Math.max(Integer.highestOneBit(rowLength) << 1, INPUT_SIZE);
          largeRowBuffer = new byte[newLargeBufferLength];
        }
        System.arraycopy(readBuffer, readOffset, largeRowBuffer, 0, remainingLength);
        int expectedPartialLength = rowLength - remainingLength;
        int partialReadLength = fileInputStream.read(largeRowBuffer, remainingLength, expectedPartialLength);
        if (partialReadLength == -1) {
          throw new IOException("Unexpected EOF (total write length " + totalWriteLength +
              ", total read length " + totalReadLength + ", read length " +
              expectedPartialLength + ")");
        }

        if (expectedPartialLength != partialReadLength) {
          throw new IOException("Unable to read a complete row of length " + rowLength +
              " (total write length " + totalWriteLength +
              ", total read length " + totalReadLength + ", read length " +
              expectedPartialLength + ", actual length " + partialReadLength + ")");
        }
        totalReadLength += partialReadLength;

        readNextBytes[readNextCount] = largeRowBuffer;
        readNextOffsets[readNextCount] = 0;
        readNextLengths[readNextCount] = rowLength;

        // Indicate we used the last row's bytes for large buffer.
        readOffset = readLength;
        readNextCount++;
        break;
      }

      readNextBytes[readNextCount] = readBuffer;
      readNextOffsets[readNextCount] = readOffset;
      readNextLengths[readNextCount] = rowLength;
      readOffset += rowLength;
      readNextCount++;

      if (readNextCount >= readNextBytes.length){
        break;
      }
      if (readLength - readOffset < 4) {
        // Handle in next round.
        break;
      }
    }
  }

  public boolean readNext() throws IOException {
    if (!isOpen) {
      return false;
    }
    if (readNextIndex >= readNextCount) {
      bufferedRead();
      // Any more left?
      if (readNextIndex >= readNextCount) {
        return false;
      }
    }

    currentBytes = readNextBytes[readNextIndex];
    currentOffset = readNextOffsets[readNextIndex];
    currentLength = readNextLengths[readNextIndex];

    readNextIndex++;
    return true;
  }

  public byte[] currentBytes() {
    return currentBytes;
  }

  public int currentOffset() {
    return currentOffset;
  }

  public int currentLength() {
    return currentLength;
  }

  public void resetWrite() throws IOException {
    if (!isOpen) {
      return;
    }

    // Truncate by re-opening FileOutputStream.
    fileOutputStream.close();
    fileOutputStream = new FileOutputStream(tmpFile);
  }

  public void clear() {
    if (fileInputStream != null) {
      try {
        fileInputStream.close();
      } catch (Throwable ignored) {
      }
      fileInputStream = null;
    }
    if (fileOutputStream != null) {
      try {
        fileOutputStream.close();
      } catch (Throwable ignored) {
      }
      fileOutputStream = null;
    }

    if (parentDir != null) {
      try {
        FileUtil.fullyDelete(parentDir);
      } catch (Throwable ignored) {
      }
    }
    parentDir = null;
    tmpFile = null;
    isOpen = false;
    totalWriteLength = 0;
  }

}
