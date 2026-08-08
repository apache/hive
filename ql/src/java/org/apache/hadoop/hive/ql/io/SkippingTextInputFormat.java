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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SkippingInputFormat is a header/footer aware input format. It truncates
 * splits identified by TextInputFormat. Header and footers are removed
 * from the splits.
 *
 * This InputFormat does NOT support Compressed Files!
 */
public class SkippingTextInputFormat extends TextInputFormat {

  private final Map<Path, Long> startIndexMap = new ConcurrentHashMap<>();
  private final Map<Path, Long> endIndexMap = new ConcurrentHashMap<>();
  private JobConf conf;
  private int headerCount;
  private int footerCount;

  @Override
  public void configure(JobConf conf) {
    this.conf = conf;
    super.configure(conf);
  }

  public void configure(JobConf conf, int headerCount, int footerCount) {
    configure(conf);
    this.headerCount = headerCount;
    this.footerCount = footerCount;
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length, String[] hosts) {
    return makeSplitInternal(file, start, length, hosts, null);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
    return makeSplitInternal(file, start, length, hosts, inMemoryHosts);
  }

  private FileSplit makeSplitInternal(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
    long cachedStart;
    long cachedEnd;
    try {
      cachedStart = getCachedStartIndex(file);
      cachedEnd = getCachedEndIndex(file);
    } catch (IOException e) {
      LOG.warn("Could not detect header/footer", e);
      return new NullRowsInputFormat.DummyInputSplit(file);
    } catch (RuntimeException e) {
      // Report unexpected detection failures clearly instead of a cryptic cast.
      throw new RuntimeException("Failed to detect header/footer boundaries for file "
          + file + " during split generation", e);
    }
    if (cachedStart > start + length) {
      return new NullRowsInputFormat.DummyInputSplit(file);
    }
    if (cachedStart > start) {
      length = length - (cachedStart - start);
      start = cachedStart;
    }
    if (cachedEnd < start) {
      return new NullRowsInputFormat.DummyInputSplit(file);
    }
    if (cachedEnd < start + length) {
      length = cachedEnd - start;
    }
    if (inMemoryHosts == null) {
      return super.makeSplit(file, start, length, hosts);
    } else {
      return super.makeSplit(file, start, length, hosts, inMemoryHosts);
    }
  }

  private long getCachedStartIndex(Path path) throws IOException {
    if (headerCount == 0) {
      return 0;
    }
    Long startIndexForFile = startIndexMap.get(path);
    if (startIndexForFile == null) {
      FileSystem fileSystem = path.getFileSystem(conf);
      // ByteCountingLineReader avoids the unreliable readLine()+getPos() idiom.
      try (FSDataInputStream fis = fileSystem.open(path)) {
        ByteCountingLineReader reader = new ByteCountingLineReader(fis);
        long currPos = 0;
        int delimiterIdx = -1;
        for (int j = 0; j < headerCount; j++) {
          String headerLine = reader.readLine();
          if (headerLine == null) {
            startIndexMap.put(path, Long.MAX_VALUE);
            return Long.MAX_VALUE;
          }
          if (j == headerCount-1) {
            String delimiter = conf.get("textinputformat.record.delimiter");
            // If record delimiter is defined
            if (delimiter != null && !delimiter.isEmpty()) {
              delimiterIdx = headerLine.indexOf(delimiter);
            } else {
              currPos = reader.getBytesConsumed();
            }
          } else {
            currPos = reader.getBytesConsumed();
          }
        }
        // Readers skip the entire first row if the start index of the
        // split is not zero. We are setting the start of the index as
        // the last byte of the previous row so the last line of header
        // is discarded instead of the first valid input row.
        // We consider record delimiters if they exist.
        startIndexForFile = currPos + delimiterIdx;
      }
      startIndexMap.put(path, startIndexForFile);
    }
    return startIndexForFile;
  }

  private long getCachedEndIndex(Path path) throws IOException {
    Long endIndexForFile = endIndexMap.get(path);
    if (endIndexForFile == null) {
      final long bufferSectionSize = 5 * 1024;
      FileSystem fileSystem = path.getFileSystem(conf);
      long endOfFile = fileSystem.getFileStatus(path).getLen();
      if (footerCount == 0) {
        endIndexForFile = endOfFile;
      } else {
        long bufferSectionEnd = endOfFile; // first byte that is not included in the section
        long bufferSectionStart = Math.max(0, bufferSectionEnd - bufferSectionSize);

        // we need 'footer count' lines and one space for EOF
        LineBuffer buffer = new LineBuffer(footerCount + 1);
        try (FSDataInputStream fis = fileSystem.open(path)) {
          while (bufferSectionEnd > bufferSectionStart) {
            fis.seek(bufferSectionStart);
            // Fresh reader per seek; offsets are seek position + bytes consumed.
            ByteCountingLineReader reader = new ByteCountingLineReader(fis);
            long pos = bufferSectionStart;
            while (pos < bufferSectionEnd) {
              if (reader.readLine() == null) {
                // if there is not enough lines in this section, check the previous
                // section. If this is the beginning section, there are simply not
                // enough lines in the file.
                break;
              }
              pos = bufferSectionStart + reader.getBytesConsumed();
              buffer.consume(pos, bufferSectionEnd);
            }
            if (buffer.getRemainingLineCount() == 0) {
              // if we consumed all the required line ends, that means the buffer now
              // contains the index of the first byte of the footer.
              break;
            } else {
              bufferSectionEnd = bufferSectionStart;
              bufferSectionStart = Math.max(0, bufferSectionEnd - bufferSectionSize);
            }
          }
          if (buffer.getRemainingLineCount() == 0) {
            // buffer.getFirstLineStart() is the first byte of the footer. So the split
            // must end before this.
            endIndexForFile = buffer.getFirstLineStart() - 1;
          } else {
            // there were not enough lines in the file to consume all footer rows.
            endIndexForFile = Long.MIN_VALUE;
          }
        }
      }
      endIndexMap.put(path, endIndexForFile);
    }
    return endIndexForFile;
  }

  /**
   * Reads lines while counting bytes consumed, so offsets can be computed without
   * {@link FSDataInputStream#getPos()} -- which is unreliable after {@code readLine()}:
   * a lone {@code '\r'} makes it swap in a non-Seekable {@code PushbackInputStream}, so
   * the next {@code getPos()} throws {@code ClassCastException}. Handles {@code '\n'},
   * {@code '\r\n'} and lone {@code '\r'}; after {@link #readLine()},
   * {@link #getBytesConsumed()} is the offset just past the line's terminator.
   */
  static final class ByteCountingLineReader {
    private final InputStream in;
    private long bytesConsumed;
    // Look-ahead byte past a '\r' (belongs to the next line, not yet counted); -1 = empty.
    private int pushedBack = -1;

    ByteCountingLineReader(InputStream in) {
      this.in = in;
    }

    long getBytesConsumed() {
      return bytesConsumed;
    }

    private int nextByte() throws IOException {
      if (pushedBack != -1) {
        int b = pushedBack;
        pushedBack = -1;
        return b;
      }
      return in.read();
    }

    /** Returns the next line without its terminator, or {@code null} at end of stream. */
    String readLine() throws IOException {
      int c = nextByte();
      if (c == -1) {
        return null;
      }
      StringBuilder sb = new StringBuilder();
      while (c != -1) {
        bytesConsumed++;
        if (c == '\n') {
          return sb.toString();
        }
        if (c == '\r') {
          int next = nextByte();
          if (next == '\n') {
            bytesConsumed++;
          } else if (next != -1) {
            pushedBack = next; // belongs to the next line, not yet counted
          }
          return sb.toString();
        }
        sb.append((char) c);
        c = nextByte();
      }
      return sb.toString();
    }
  }

  static class LineBuffer {
    private final Queue<Long> queue = new ArrayDeque<Long>();
    private int remainingLineEnds;
    private long lowPosition = Long.MAX_VALUE;

    LineBuffer(int requiredLines) {
      this.remainingLineEnds = requiredLines;
    }

    public void consume(long position, long sectionEnd) {
      if (position > sectionEnd) {
        return;
      }
      if (position < lowPosition) {
        remainingLineEnds -= queue.size();
        queue.clear();
        queue.add(position);
        lowPosition = position;
      } else if (position > lowPosition) {
        if (queue.size() == remainingLineEnds) {
          queue.poll();
        }
        queue.add(position);
        lowPosition = queue.peek();
      }
    }

    public int getRemainingLineCount() {
      return remainingLineEnds - queue.size();
    }

    public long getFirstLineStart() {
      return lowPosition;
    }
  }
}
