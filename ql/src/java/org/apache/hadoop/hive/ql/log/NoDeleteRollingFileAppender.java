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

package org.apache.hadoop.hive.ql.log;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Writer;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.CountingQuietWriter;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.helpers.OptionConverter;
import org.apache.log4j.spi.LoggingEvent;

public class NoDeleteRollingFileAppender extends FileAppender {
  /**
   * The default maximum file size is 10MB.
   */
  protected long maxFileSize = 10 * 1024 * 1024;

  private long nextRollover = 0;

  /**
   * The default constructor simply calls its {@link FileAppender#FileAppender
   * parents constructor}.
   */
  public NoDeleteRollingFileAppender() {
  }

  /**
   * Instantiate a RollingFileAppender and open the file designated by
   * <code>filename</code>. The opened filename will become the output
   * destination for this appender.
   * <p>
   * If the <code>append</code> parameter is true, the file will be appended to.
   * Otherwise, the file designated by <code>filename</code> will be truncated
   * before being opened.
   */
  public NoDeleteRollingFileAppender(Layout layout, String filename,
      boolean append) throws IOException {
    super(layout, filename, append);
  }

  /**
   * Instantiate a FileAppender and open the file designated by
   * <code>filename</code>. The opened filename will become the output
   * destination for this appender.
   * <p>
   * The file will be appended to.
   */
  public NoDeleteRollingFileAppender(Layout layout, String filename)
      throws IOException {
    super(layout, filename);
  }

  /**
   * Get the maximum size that the output file is allowed to reach before being
   * rolled over to backup files.
   */
  public long getMaximumFileSize() {
    return maxFileSize;
  }

  /**
   * Implements the usual roll over behavior.
   * <p>
   * <code>File</code> is renamed <code>File.yyyyMMddHHmmss</code> and closed. A
   * new <code>File</code> is created to receive further log output.
   */
  // synchronization not necessary since doAppend is already synced
  public void rollOver() {
    if (qw != null) {
      long size = ((CountingQuietWriter) qw).getCount();
      LogLog.debug("rolling over count=" + size);
      // if operation fails, do not roll again until
      // maxFileSize more bytes are written
      nextRollover = size + maxFileSize;
    }

    this.closeFile(); // keep windows happy.

    int p = fileName.lastIndexOf(".");
    String file = p > 0 ? fileName.substring(0, p) : fileName;
    try {
      // This will also close the file. This is OK since multiple
      // close operations are safe.
      this.setFile(file, false, bufferedIO, bufferSize);
      nextRollover = 0;
    } catch (IOException e) {
      if (e instanceof InterruptedIOException) {
        Thread.currentThread().interrupt();
      }
      LogLog.error("setFile(" + file + ", false) call failed.", e);
    }
  }

  public synchronized void setFile(String fileName, boolean append,
      boolean bufferedIO, int bufferSize) throws IOException {
    String newFileName = getLogFileName(fileName);
    super.setFile(newFileName, append, this.bufferedIO, this.bufferSize);
    if (append) {
      File f = new File(newFileName);
      ((CountingQuietWriter) qw).setCount(f.length());
    }
  }

  /**
   * Set the maximum size that the output file is allowed to reach before being
   * rolled over to backup files.
   * <p>
   * This method is equivalent to {@link #setMaxFileSize} except that it is
   * required for differentiating the setter taking a <code>long</code> argument
   * from the setter taking a <code>String</code> argument by the JavaBeans
   * {@link java.beans.Introspector Introspector}.
   *
   * @see #setMaxFileSize(String)
   */
  public void setMaximumFileSize(long maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

  /**
   * Set the maximum size that the output file is allowed to reach before being
   * rolled over to backup files.
   * <p>
   * In configuration files, the <b>MaxFileSize</b> option takes an long integer
   * in the range 0 - 2^63. You can specify the value with the suffixes "KB",
   * "MB" or "GB" so that the integer is interpreted being expressed
   * respectively in kilobytes, megabytes or gigabytes. For example, the value
   * "10KB" will be interpreted as 10240.
   */
  public void setMaxFileSize(String value) {
    maxFileSize = OptionConverter.toFileSize(value, maxFileSize + 1);
  }

  protected void setQWForFiles(Writer writer) {
    this.qw = new CountingQuietWriter(writer, errorHandler);
  }

  /**
   * This method differentiates RollingFileAppender from its super class.
   */
  protected void subAppend(LoggingEvent event) {
    super.subAppend(event);

    if (fileName != null && qw != null) {
      long size = ((CountingQuietWriter) qw).getCount();
      if (size >= maxFileSize && size >= nextRollover) {
        rollOver();
      }
    }
  }

  // Mangled file name. Append the current timestamp
  private static String getLogFileName(String oldFileName) {
    return oldFileName + "." + Long.toString(System.currentTimeMillis());
  }
}
