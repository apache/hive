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
package org.apache.hadoop.hive.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An exception handler chain that process the input exception by going through
 * all exceptions defined in this chain one by one until either one exception
 * handler returns true or it reaches the end of the chain. If it reaches the
 * end of the chain, and still no exception handler returns true, throw the
 * exception to the caller.
 */
public class HiveIOExceptionHandlerChain {

  public static String HIVE_IO_EXCEPTION_HANDLE_CHAIN = "hive.io.exception.handlers";

  @SuppressWarnings("unchecked")
  public static HiveIOExceptionHandlerChain getHiveIOExceptionHandlerChain(
      JobConf conf) {
    HiveIOExceptionHandlerChain chain = new HiveIOExceptionHandlerChain();
    String exceptionHandlerStr = conf.get(HIVE_IO_EXCEPTION_HANDLE_CHAIN);
    List<HiveIOExceptionHandler> handlerChain = new ArrayList<HiveIOExceptionHandler>();
    if (exceptionHandlerStr != null && !exceptionHandlerStr.trim().equals("")) {
      String[] handlerArr = exceptionHandlerStr.split(",");
      if (handlerArr != null && handlerArr.length > 0) {
        for (String handlerStr : handlerArr) {
          if (!handlerStr.trim().equals("")) {
            try {
              Class<? extends HiveIOExceptionHandler> handlerCls =
                (Class<? extends HiveIOExceptionHandler>) Class.forName(handlerStr);
              HiveIOExceptionHandler handler = ReflectionUtils.newInstance(handlerCls, null);
              handlerChain.add(handler);
            } catch (Exception e) {
            }
          }
        }
      }
    }

    chain.setHandlerChain(handlerChain);
    return chain;
  }

  private List<HiveIOExceptionHandler> handlerChain;

  /**
   * @return the exception handler chain defined
   */
  protected List<HiveIOExceptionHandler> getHandlerChain() {
    return handlerChain;
  }

  /**
   * set the exception handler chain
   * @param handlerChain
   */
  protected void setHandlerChain(List<HiveIOExceptionHandler> handlerChain) {
    this.handlerChain = handlerChain;
  }

  public RecordReader<?,?>  handleRecordReaderCreationException(Exception e) throws IOException {
    RecordReader<?, ?> ret = null;

    if (handlerChain != null && handlerChain.size() > 0) {
      for (HiveIOExceptionHandler handler : handlerChain) {
        ret = handler.handleRecordReaderCreationException(e);
        if (ret != null) {
          return ret;
        }
      }
    }

    //re-throw the exception as an IOException
    throw new IOException(e);
  }

  /**
   * This is to handle exception when doing next operations. Here we use a
   * HiveIOExceptionNextHandleResult to store the results of each handler. If
   * the exception is handled by one handler, the handler should set
   * HiveIOExceptionNextHandleResult to be handled, and also set the handle
   * result. The handle result is used to return the reader's next to determine
   * if need to open a new file for read or not.
   */
  public boolean handleRecordReaderNextException(Exception e)
      throws IOException {
    HiveIOExceptionNextHandleResult result = new HiveIOExceptionNextHandleResult();
    if (handlerChain != null && handlerChain.size() > 0) {
      for (HiveIOExceptionHandler handler : handlerChain) {
        handler.handleRecorReaderNextException(e, result);
        if (result.getHandled()) {
          return result.getHandleResult();
        }
      }
    }

    //re-throw the exception as an IOException
    throw new IOException(e);
  }

}
