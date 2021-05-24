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

package org.apache.hadoop.hive.ql.exec.repl.atlas;

import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.atlas.AtlasServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Implement retry logic for service calls.
 */
public class RetryingClient {
  private static final Logger LOG = LoggerFactory.getLogger(RetryingClient.class);
  private static final int PAUSE_DURATION_INCREMENT_IN_MINUTES_DEFAULT = (30 * 1000);
  private static final int RETRY_COUNT_DEFAULT = 5;
  private static final String ERROR_MESSAGE_NO_ENTITIES = "no entities to create/update";
  private static final String ERROR_MESSAGE_IN_PROGRESS = "import or export is in progress";
  private static final String ATLAS_ERROR_CODE_IMPORT_EMPTY_ZIP = "empty ZIP file";
  private static final int MAX_RETY_COUNT = RETRY_COUNT_DEFAULT;
  private static final int PAUSE_DURATION_INCREMENT_IN_MS = PAUSE_DURATION_INCREMENT_IN_MINUTES_DEFAULT;

  protected <T> T invokeWithRetry(Callable<T> func, T defaultReturnValue) throws Exception {
    for (int currentRetryCount = 1; currentRetryCount <= MAX_RETY_COUNT; currentRetryCount++) {
      try {
        LOG.debug("Retrying method: {}", func.getClass().getName(), null);
        return func.call();
      } catch (Exception e) {
        if (processImportExportLockException(e, currentRetryCount)) {
          continue;
        }
        if (processInvalidParameterException(e)) {
          return null;
        }
        LOG.error(func.getClass().getName(), e);
        throw new Exception(e);
      }
    }
    return defaultReturnValue;
  }

  private boolean processInvalidParameterException(Exception e) {
    if (e instanceof UniformInterfaceException) {
      return true;
    }
    if (!(e instanceof AtlasServiceException)) {
      return false;
    }
    if (e.getMessage() == null) {
      return false;
    }
    return (e.getMessage().contains(ERROR_MESSAGE_NO_ENTITIES)
            || e.getMessage().contains(ATLAS_ERROR_CODE_IMPORT_EMPTY_ZIP));
  }

  private boolean processImportExportLockException(Exception e, int currentRetryCount) throws Exception {
    if (!(e instanceof AtlasServiceException)) {
      return false;
    }
    String excMessage = e.getMessage() == null ? "" : e.getMessage();
    if (excMessage.contains(ERROR_MESSAGE_IN_PROGRESS)) {
      try {
        int pauseDuration = PAUSE_DURATION_INCREMENT_IN_MS * currentRetryCount;
        LOG.info("Atlas in-progress operation detected. Will pause for: {} ms", pauseDuration);
        Thread.sleep(pauseDuration);
      } catch (InterruptedException intEx) {
        LOG.error("Pause wait interrupted!", intEx);
        throw new Exception(intEx);
      }
      return true;
    }
    return false;
  }
}
