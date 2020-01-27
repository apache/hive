package org.apache.hadoop.hive.ql.log;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.RandomAccessFileManager;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginConfiguration;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.net.Advertiser;
import org.apache.logging.log4j.core.util.Booleans;
import org.apache.logging.log4j.core.util.Integers;

/**
 * A File Appender that does not append log records after it has been stopped.
 * Based on
 * https://github.com/apache/logging-log4j2/blob/c02e7de69e81c174f1ea0be43639d9e231fa5283/log4j-core/src/main/java/org/apache/logging/log4j/core/appender/RandomAccessFileAppender.java
 * (which is from log4j 2.6.2)
 */
@Plugin(name = "HushableRandomAccessFile", category = "Core", elementType = "appender", printObject = true)
public final class HushableRandomAccessFileAppender extends
    AbstractOutputStreamAppender<RandomAccessFileManager> {

  private static final LoadingCache<String, String> CLOSED_FILES =
      CacheBuilder.newBuilder().maximumSize(1000)
          .expireAfterWrite(1, TimeUnit.SECONDS)
          .build(new CacheLoader<String, String>() {
            @Override
            public String load(String key) throws Exception {
              return key;
            }
          });


  private final String fileName;
  private Object advertisement;
  private final Advertiser advertiser;

  private HushableRandomAccessFileAppender(final String name, final Layout<? extends Serializable> layout,
      final Filter filter, final RandomAccessFileManager manager, final String filename,
      final boolean ignoreExceptions, final boolean immediateFlush, final Advertiser advertiser) {

    super(name, layout, filter, ignoreExceptions, immediateFlush, manager);
    if (advertiser != null) {
      final Map<String, String> configuration = new HashMap<>(
          layout.getContentFormat());
      configuration.putAll(manager.getContentFormat());
      configuration.put("contentType", layout.getContentType());
      configuration.put("name", name);
      advertisement = advertiser.advertise(configuration);
    }
    this.fileName = filename;
    this.advertiser = advertiser;
  }

  @Override
  public void stop() {
    super.stop();
    CLOSED_FILES.put(fileName, fileName);
    if (advertiser != null) {
      advertiser.unadvertise(advertisement);
    }
  }

  /**
   * Write the log entry rolling over the file when required.
   *
   * @param event The LogEvent.
   */
  @Override
  public void append(final LogEvent event) {
    // Unlike RandomAccessFileAppender, do not append log when stopped.
    if (isStopped()) {
      // Don't try to log anything when appender is stopped
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
   * Returns the file name this appender is associated with.
   *
   * @return The File name.
   */
  public String getFileName() {
    return this.fileName;
  }

  /**
   * Returns the size of the file manager's buffer.
   * @return the buffer size
   */
  public int getBufferSize() {
    return getManager().getBufferSize();
  }

  // difference from standard File Appender:
  // locking is not supported and buffering cannot be switched off
  /**
   * Create a File Appender.
   *
   * @param fileName The name and path of the file.
   * @param append "True" if the file should be appended to, "false" if it
   *            should be overwritten. The default is "true".
   * @param name The name of the Appender.
   * @param immediateFlush "true" if the contents should be flushed on every
   *            write, "false" otherwise. The default is "true".
   * @param bufferSizeStr The buffer size, defaults to {@value RandomAccessFileManager#DEFAULT_BUFFER_SIZE}.
   * @param ignore If {@code "true"} (default) exceptions encountered when appending events are logged; otherwise
   *               they are propagated to the caller.
   * @param layout The layout to use to format the event. If no layout is
   *            provided the default PatternLayout will be used.
   * @param filter The filter, if any, to use.
   * @param advertise "true" if the appender configuration should be
   *            advertised, "false" otherwise.
   * @param advertiseURI The advertised URI which can be used to retrieve the
   *            file contents.
   * @param config The Configuration.
   * @return The FileAppender.
   */
  @PluginFactory
  public static HushableRandomAccessFileAppender createAppender(
      @PluginAttribute("fileName") final String fileName,
      @PluginAttribute("append") final String append,
      @PluginAttribute("name") final String name,
      @PluginAttribute("immediateFlush") final String immediateFlush,
      @PluginAttribute("bufferSize") final String bufferSizeStr,
      @PluginAttribute("ignoreExceptions") final String ignore,
      @PluginElement("Layout") Layout<? extends Serializable> layout,
      @PluginElement("Filter") final Filter filter,
      @PluginAttribute("advertise") final String advertise,
      @PluginAttribute("advertiseURI") final String advertiseURI,
      @PluginConfiguration final Configuration config) {

    final boolean isAppend = Booleans.parseBoolean(append, true);
    final boolean isFlush = Booleans.parseBoolean(immediateFlush, true);
    final boolean ignoreExceptions = Booleans.parseBoolean(ignore, true);
    final boolean isAdvertise = Boolean.parseBoolean(advertise);
    final int bufferSize = Integers.parseInt(bufferSizeStr, 256 * 1024 /* RandomAccessFileManager.DEFAULT_BUFFER_SIZE */);

    if (name == null) {
      LOGGER.error("No name provided for FileAppender");
      return null;
    }

    if (fileName == null) {
      LOGGER.error("No filename provided for FileAppender with name "
          + name);
      return null;
    }

    /**
     * In corner cases (e.g exceptions), there seem to be some race between
     * com.lmax.disruptor.BatchEventProcessor and HS2 thread which is actually
     * stopping the logs. Because of this, same filename is recreated and
     * stop() would never be invoked on that instance, causing a mem leak.
     * To prevent same file being recreated within very short time,
     * CLOSED_FILES are tracked in cache with TTL of 1 second. This
     * also helps in avoiding the stale directories created.
     */
    if (CLOSED_FILES.getIfPresent(fileName) != null) {
      // Do not create another file, which got closed in last 5 seconds
      LOGGER.error(fileName + " was closed recently.");
      return null;
    }

    if (layout == null) {
      layout = PatternLayout.createDefaultLayout();
    }
    final RandomAccessFileManager manager = RandomAccessFileManager.getFileManager(
        fileName, isAppend, isFlush, bufferSize, advertiseURI, layout, config
    );
    if (manager == null) {
      return null;
    }

    return new HushableRandomAccessFileAppender(
        name, layout, filter, manager, fileName, ignoreExceptions, isFlush,
        isAdvertise ? config.getAdvertiser() : null
    );
  }
}
