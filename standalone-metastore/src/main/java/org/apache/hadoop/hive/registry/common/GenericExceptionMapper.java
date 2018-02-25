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
package org.apache.hadoop.hive.registry.common;

import org.apache.hadoop.hive.registry.common.exception.service.exception.WebServiceException;
import org.apache.hadoop.hive.registry.common.exception.service.exception.request.BadRequestException;
import org.apache.hadoop.hive.registry.common.exception.service.exception.server.UnhandledServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Throwable> {
    private static final Logger LOG = LoggerFactory.getLogger(GenericExceptionMapper.class);

    @Override
    public Response toResponse(Throwable ex) {
        if (ex instanceof ProcessingException
                || ex instanceof IllegalArgumentException
                || ex instanceof NullPointerException) {
            return BadRequestException.of().getResponse();
        } else if (ex instanceof WebServiceException) {
            return ((WebServiceException) ex).getResponse();
        }

        logUnhandledException(ex);
        return new UnhandledServerException(ex.getMessage()).getResponse();
    }

    private void logUnhandledException(Throwable ex) {
        String errMessage = String.format("Got exception: [%s] / message [%s]",
                                          ex.getClass().getSimpleName(), ex.getMessage());
        StackTraceElement elem = findFirstResourceCallFromCallStack(ex.getStackTrace());
        String resourceClassName = null;
        if (elem != null) {
            errMessage += String.format(" / related resource location: [%s.%s](%s:%d)",
                                        elem.getClassName(), elem.getMethodName(), elem.getFileName(), elem.getLineNumber());
            resourceClassName = elem.getClassName();
        }

        Logger log = getEffectiveLogger(resourceClassName);
        log.error(errMessage, ex);
    }

    private StackTraceElement findFirstResourceCallFromCallStack(StackTraceElement[] stackTrace) {
        for (StackTraceElement stackTraceElement : stackTrace) {
            try {
                Class<?> aClass = Class.forName(stackTraceElement.getClassName());
                Path pathAnnotation = aClass.getAnnotation(Path.class);

                if (pathAnnotation != null) {
                    return stackTraceElement;
                }
            } catch (ClassNotFoundException e) {
                // skip
            }
        }

        return null;
    }

    private Logger getEffectiveLogger(String resourceClassName) {
        Logger log = LOG;
        if (resourceClassName != null) {
            log = LoggerFactory.getLogger(resourceClassName);
        }
        return log;
    }

}

