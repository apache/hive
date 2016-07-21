/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.RandomAccessFileAppender;
import org.apache.logging.log4j.core.config.AppenderControl;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginConfiguration;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.config.plugins.PluginNode;
import org.apache.logging.log4j.util.Strings;

/**
 * An appender to wrap around RandomAccessFileAppender, and rename the file once the appender is
 * closed.
 * Could be potentially extended to other appenders by special casing them to figure out how files
 * are to be handled.
 */
@Plugin(name = "LlapWrappedAppender", category = "Core",
    elementType = "appender", printObject = true, deferChildren = true)
public class LlapWrappedAppender extends AbstractAppender {

  private static final boolean DEFAULT_RENAME_FILES_ON_CLOSE = true;
  private static final String DEFAULT_RENAMED_FILE_SUFFIX = ".done";

  private final Node node;
  private final Configuration config;
  private final boolean renameFileOnClose;
  private final String renamedFileSuffix;

  private AtomicReference<Appender> realAppender = new AtomicReference<>();
  private AtomicReference<AppenderControl> appenderControl = new AtomicReference<>();

  public LlapWrappedAppender(final String name, final Node node, final Configuration config,
                             boolean renameOnClose, String renamedFileSuffix) {
    super(name, null, null);
    this.node = node;
    this.config = config;
    this.renameFileOnClose = renameOnClose;
    this.renamedFileSuffix = renamedFileSuffix;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          LlapWrappedAppender.class.getName() + " created with name=" + name + ", renameOnClose=" +
              renameOnClose + ", renamedFileSuffix=" + renamedFileSuffix);
    }
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void append(LogEvent event) {
    setupAppenderIfRequired(event);
    if (appenderControl.get() != null) {
      if (!(event.getMarker() != null && event.getMarker().getName() != null &&
          event.getMarker().getName().equals(Log4jQueryCompleteMarker.EOF_MARKER))) {
        appenderControl.get().callAppender(event);
      } else {
        LOGGER.debug("Not forwarding message with maker={}, marker.getName={}", event.getMarker(),
            (event.getMarker() == null ? "nullMarker" : event.getMarker().getName()));
      }
    }
  }

  private void setupAppenderIfRequired(LogEvent event) {
    if (appenderControl.get() == null) {
      if (node.getType().getElementName().equalsIgnoreCase("appender")) {
        for (final Node cnode : node.getChildren()) {
          final Node appNode = new Node(cnode);
          config.createConfiguration(appNode, event);
          if (appNode.getObject() instanceof Appender) {
            final Appender app = appNode.getObject();
            app.start();
            if (!(app instanceof RandomAccessFileAppender)) {
              String message =
                  "Cannot handle appenders other than " + RandomAccessFileAppender.class.getName() +
                      ". Found: " + app.getClass().getName();
              LOGGER.error(message);
              throw new IllegalStateException(message);
            }
            realAppender.set(app);
            appenderControl.set(new AppenderControl(app, null, null));
            if (LOGGER.isDebugEnabled()) {
              RandomAccessFileAppender raf = (RandomAccessFileAppender) app;
              LOGGER.debug(
                  "Setup new appender to write to file: " + raf.getFileName() + ", appenderName=" +
                      raf.getName() + ", appenderManagerName=" + raf.getManager().getName());

            }
            break;
          }
        }
        if (appenderControl.get() == null) {
          // Fail if mis-configured.
          throw new RuntimeException(LlapWrappedAppender.class.getSimpleName() +
              "name=" + getName() + " unable to setup actual appender." +
              "Could not find child appender");
        }
      } else {
        // Fail if mis-configured.
        throw new RuntimeException(LlapWrappedAppender.class.getSimpleName() +
            "name=" + getName() + " unable to setup actual appender." +
            "Could not find child appender");
      }
    }
  }


  @Override
  public void stop() {
    if (!(this.isStopping() || this.isStopped())) {
      super.stop();
      if (appenderControl.get() != null) {
        appenderControl.get().stop();
        realAppender.get().stop();
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Stop invoked for " + ((RandomAccessFileAppender) realAppender.get()).getFileName());
      }
      if (realAppender.get() == null) {
        LOGGER.info("RealAppender is null. Ignoring stop");
        return;
      }

      RandomAccessFileAppender raf = (RandomAccessFileAppender) realAppender.get();
      Path renamedPath = null;
      if (renameFileOnClose) {
        try {
          // Look for a file to which we can move the existing file. With external services,
          // it's possible for the service to be marked complete after each fragment.
          int counter = 0;
          while(true) {
            renamedPath = getRenamedPath(raf.getFileName(), counter);
            if (!Files.exists(renamedPath)) {
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Renaming file: " + raf.getFileName() + " to " + renamedPath);
              }
              Files.move(Paths.get(raf.getFileName()), renamedPath);
              break;
            }
            counter++;
          }
        } catch (IOException e) {
          // Bail on an exception - out of the loop.
          LOGGER.warn("Failed to rename file: " + raf.getFileName() + " to " + renamedPath, e);
        }
      }
    }
  }

  private Path getRenamedPath(String originalFileName, int iteration) {
    Path renamedPath;
    if (iteration == 0) {
      renamedPath = Paths.get(originalFileName + renamedFileSuffix);
    } else {
      renamedPath = Paths.get(originalFileName + "." + iteration + renamedFileSuffix);
    }
    return renamedPath;
  }

  @PluginFactory
  public static LlapWrappedAppender createAppender(
      @PluginAttribute("name") final String name, // This isn't really used for anything.
      @PluginAttribute("renameFileOnClose") final String renameFileOnCloseProvided,
      @PluginAttribute("renamedFileSuffix") final String renamedFileSuffixProvided,
      @PluginNode final Node node,
      @PluginConfiguration final Configuration config
  ) {
    if (config == null) {
      LOGGER.error("PluginConfiguration not expected to be null");
      return null;
    }
    if (node == null) {
      LOGGER.error("Node must be specified as an appender specification");
      return null;
    }

    boolean renameFileOnClose = DEFAULT_RENAME_FILES_ON_CLOSE;
    if (Strings.isNotBlank(renameFileOnCloseProvided)) {
      renameFileOnClose = Boolean.parseBoolean(renameFileOnCloseProvided);
    }
    String renamedFileSuffix = DEFAULT_RENAMED_FILE_SUFFIX;
    if (Strings.isNotBlank(renamedFileSuffixProvided)) {
      renamedFileSuffix = renamedFileSuffixProvided;
    }

    return new LlapWrappedAppender(name, node, config, renameFileOnClose, renamedFileSuffix);
  }

}