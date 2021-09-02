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
package org.apache.hadoop.hive.llap.log;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.RandomAccessFileManager;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A random access file appender that conditionally renames the file when the appender is stopped.
 *
 * If the appender receives an event with a {@link Log4jQueryCompleteMarker} then the underlying file will be renamed
 * (using the ".done" suffix) when the appender is stopped. 
 *
 * Moreover, the appender filters events with {@link Log4jQueryCompleteMarker} so they never appear in the logs.
 */
@Plugin(name = "LlapRandomAccessFileAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class LlapRandomAccessFileAppender extends AbstractOutputStreamAppender<RandomAccessFileManager> {

  /**
   * Warning: Changing the file suffix may break external clients if it is not accompanied by appropriate changes.
   * Two known dependencies are the following:
   * <li>
   *   <ul>YARN rolling log aggregation relies on this naming convention to function correctly (see HIVE-14225)</ul>
   *   <ul>Tez UI checks files with these ending to display certain information to the user (TEZ-3629)</ul>
   * </li>
   */
  private static final String RENAME_SUFFIX = ".done";
  private final AtomicBoolean renameOnStop = new AtomicBoolean(false);

  protected LlapRandomAccessFileAppender(String name, Layout<? extends Serializable> layout, Filter filter,
      boolean ignoreExceptions, boolean immediateFlush, RandomAccessFileManager manager) {
    super(name, layout, filter, ignoreExceptions, immediateFlush, manager);
  }

  /**
   * Builds LlapRandomAccessFileAppender instances.
   */
  public static class Builder<B extends LlapRandomAccessFileAppender.Builder<B>>
      extends AbstractOutputStreamAppender.Builder<B>
      implements org.apache.logging.log4j.core.util.Builder<LlapRandomAccessFileAppender> {

    @PluginBuilderAttribute("fileName")
    private String fileName;

    @PluginBuilderAttribute("append")
    private boolean append = true;

    @Override
    public LlapRandomAccessFileAppender build() {
      final String name = getName();
      if (name == null) {
        LOGGER.error("No name provided for FileAppender");
        return null;
      }

      if (fileName == null) {
        LOGGER.error("No filename provided for FileAppender with name " + name);
        return null;
      }
      final Layout<? extends Serializable> layout = getOrCreateLayout();
      final boolean immediateFlush = isImmediateFlush();
      final RandomAccessFileManager manager =
          RandomAccessFileManager.getFileManager(fileName, append, immediateFlush, getBufferSize(), null, layout, null);
      if (manager == null) {
        return null;
      }

      return new LlapRandomAccessFileAppender(name, layout, getFilter(), isIgnoreExceptions(), immediateFlush, manager);
    }

    public B setFileName(final String fileName) {
      this.fileName = fileName;
      return asBuilder();
    }

    public B setAppend(final boolean append) {
      this.append = append;
      return asBuilder();
    }

  }

  @Override
  public boolean stop(final long timeout, final TimeUnit timeUnit) {
    setStopping();
    super.stop(timeout, timeUnit, false);
    if (renameOnStop.get()) {
      rename();
    }
    setStopped();
    return true;
  }

  private void rename() {
    String currentName = getManager().getFileName();
    Path newFileName = findRenamePath();
    try {
      LOGGER.trace("Renaming file: {} to {}", currentName, newFileName);
      Files.move(Paths.get(currentName), newFileName);
    } catch (IOException e) {
      LOGGER.error("Failed to rename file: " + currentName + " to " + newFileName, e);
    }
  }

  /**
   * Returns an available path to rename the current file.
   *
   * It avoids the case where the file exists already in the current directory that would lead to the logger 
   * failing miserably.
   */
  private Path findRenamePath() {
    Path renamedPath;
    int i = 0;
    do {
      String suffix = i == 0 ? RENAME_SUFFIX : i + RENAME_SUFFIX;
      renamedPath = Paths.get(getManager().getFileName() + suffix);
      i++;
    } while (Files.exists(renamedPath));
    return renamedPath;
  }

  /**
   * Appends the log entry to the file when required.
   *
   * @param event The LogEvent.
   */
  @Override
  public void append(final LogEvent event) {
    if (event.getMarker() != null && event.getMarker().getName().equals(Log4jQueryCompleteMarker.EOF_MARKER)) {
      renameOnStop.set(true);
      return;
    }
    // Leverage the nice batching behaviour of async Loggers/Appenders:
    // we can signal the file manager that it needs to flush the buffer
    // to disk at the end of a batch.
    // From a user's point of view, this means that all log events are
    // _always_ available in the log file, without incurring the overhead
    // of immediateFlush=true.
    getManager().setEndOfBatch(event.isEndOfBatch()); // FIXME manager's EndOfBatch threadlocal can be deleted
    // LOG4J2-1292 utilize gc-free Layout.encode() method: taken care of in superclass
    super.append(event);
  }

  /**
   * Returns a builder for a LlapRandomAccessFileAppender.
   */
  @PluginBuilderFactory
  public static <B extends LlapRandomAccessFileAppender.Builder<B>> B newBuilder() {
    return new LlapRandomAccessFileAppender.Builder<B>().asBuilder();
  }

}
