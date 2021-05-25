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
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Implement retry logic for service calls.
 */
public class RetryingClientTimeBased {
  private static final long MINIMUM_DELAY_IN_SEC = 60;
  private static final Logger LOG = LoggerFactory.getLogger(RetryingClientTimeBased.class);
  private static final String ERROR_MESSAGE_NO_ENTITIES = "no entities to create/update";
  private static final String ERROR_MESSAGE_IN_PROGRESS = "import or export is in progress";
  private static final String ATLAS_ERROR_CODE_IMPORT_EMPTY_ZIP = "empty ZIP file";
  protected long totalDurationInSeconds;
  protected long initialDelayInSeconds;
  protected long maxRetryDelayInSeconds;
  protected double backOff;
  protected int maxJitterInSeconds;

  protected <T> T invokeWithRetry(Callable<T> func) throws SemanticException {
    long startTime = System.currentTimeMillis();
    long delay = this.initialDelayInSeconds;
    while (true) {
      try {
        LOG.debug("Retrying method: {}", func.getClass().getName(), null);
        return func.call();
      } catch (Exception e) {
        if (processImportExportLockException(e, delay)) {
          //retry case. compute next sleep time
          delay = getNextDelay(delay);
          if (elapsedTimeInSeconds(startTime) + delay > this.totalDurationInSeconds) {
            throw new SemanticException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(), e);
          }
          continue;
        }
        if (processInvalidParameterException(e)) {
          LOG.info("There is nothing to export/import.");
          return null;
        }
        LOG.error(func.getClass().getName(), e);
        throw new SemanticException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(), e);
      }
    }
  }

  private long getNextDelay(long currentDelay) {
    if (currentDelay <= 0) { // in case initial delay was set to 0.
      currentDelay = MINIMUM_DELAY_IN_SEC;
    }

    currentDelay *= this.backOff;
    if (this.maxJitterInSeconds > 0) {
      currentDelay += new Random().nextInt(this.maxJitterInSeconds);
    }

    if (currentDelay > this.maxRetryDelayInSeconds) {
      currentDelay = this.maxRetryDelayInSeconds;
    }

    return  currentDelay;
  }


  private long elapsedTimeInSeconds(long fromTimeMillis) {
    return (System.currentTimeMillis() - fromTimeMillis)/ 1000;
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

  private boolean processImportExportLockException(Exception e, long delay) throws SemanticException {
    if (!(e instanceof AtlasServiceException)) {
      return false;
    }
    String excMessage = e.getMessage() == null ? "" : e.getMessage();
    if (excMessage.contains(ERROR_MESSAGE_IN_PROGRESS)) {
      try {
        LOG.info("Atlas in-progress operation detected. Will pause for: {} seconds", delay);
        Thread.sleep(delay * 1000L);
      } catch (InterruptedException intEx) {
        LOG.error("Pause wait interrupted!", intEx);
        throw new SemanticException(intEx);
      }
      return true;
    }
    return false;
  }
}
