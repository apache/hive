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
      FileSystem fileSystem;
      FSDataInputStream fis = null;
      fileSystem = path.getFileSystem(conf);
      try {
        fis = fileSystem.open(path);
        for (int j = 0; j < headerCount; j++) {
          if (fis.readLine() == null) {
            startIndexMap.put(path, Long.MAX_VALUE);
            return Long.MAX_VALUE;
          }
        }
        // Readers skip the entire first row if the start index of the
        // split is not zero. We are setting the start of the index as
        // the last byte of the previous row so the last line of header
        // is discarded instead of the first valid input row.
        startIndexForFile = fis.getPos() - 1;
      } finally {
        if (fis != null) {
          fis.close();
        }
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
        FSDataInputStream fis = null;
        try {
          fis = fileSystem.open(path);
          while (bufferSectionEnd > bufferSectionStart) {
            fis.seek(bufferSectionStart);
            long pos = fis.getPos();
            while (pos < bufferSectionEnd) {
              if (fis.readLine() == null) {
                // if there is not enough lines in this section, check the previous
                // section. If this is the beginning section, there are simply not
                // enough lines in the file.
                break;
              }
              pos = fis.getPos();
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
        } finally {
          if (fis != null) {
            fis.close();
          }
        }
      }
      endIndexMap.put(path, endIndexForFile);
    }
    return endIndexForFile;
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
