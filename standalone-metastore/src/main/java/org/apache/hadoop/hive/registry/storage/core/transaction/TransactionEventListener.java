/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.registry.storage.core.transaction;

import org.apache.hadoop.hive.registry.common.transaction.TransactionIsolation;
import org.apache.hadoop.hive.registry.common.transaction.UnitOfWork;
import org.apache.hadoop.hive.registry.storage.core.TransactionManager;
import org.glassfish.jersey.server.model.ResourceMethod;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TransactionEventListener implements ApplicationEventListener {

    private final ConcurrentMap<ResourceMethod, Optional<UnitOfWork>> methodMap = new ConcurrentHashMap<>();
    private final TransactionManager transactionManager;
    private final boolean runWithTxnIfNotConfigured;

    /**
     * Creates instance by taking the below arguments and webservice methods are not run in transaction unless they use
     * {@link UnitOfWork} on respective webservice resource methods.
     *
     * @param transactionManager transactionManager to be used for txn lifecycle invocations.
     */
    public TransactionEventListener(TransactionManager transactionManager) {
        this(transactionManager, false);
    }

    /**
     * Creates instance by taking the below arguments.
     *
     * @param transactionManager        transactionManager to be used for txn lifecycle invocations.
     * @param runWithTxnIfNotConfigured All webservice resource methods are invoked in a transaction even if they are
     *                                  not set with {@link UnitOfWork}.
     */
    public TransactionEventListener(TransactionManager transactionManager, boolean runWithTxnIfNotConfigured) {
        this.transactionManager = transactionManager;
        this.runWithTxnIfNotConfigured = runWithTxnIfNotConfigured;
    }

    private static class UnitOfWorkEventListener implements RequestEventListener {
        private final ConcurrentMap<ResourceMethod, Optional<UnitOfWork>> methodMap;
        private final TransactionManager transactionManager;
        private final boolean runWithTxnIfNotConfigured;
        private boolean useTransactionForUnitOfWork = true;
        private boolean isTransactionActive = false;

        public UnitOfWorkEventListener(ConcurrentMap<ResourceMethod, Optional<UnitOfWork>> methodMap,
                                       TransactionManager transactionManager,
                                       boolean runWithTxnIfNotConfigured) {
            this.methodMap = methodMap;
            this.transactionManager = transactionManager;
            this.runWithTxnIfNotConfigured = runWithTxnIfNotConfigured;
        }

        @Override
        public void onEvent(RequestEvent event) {
            final RequestEvent.Type eventType = event.getType();
            if (eventType == RequestEvent.Type.RESOURCE_METHOD_START) {
                Optional<UnitOfWork> unitOfWork = methodMap.computeIfAbsent(event.getUriInfo()
                                                                                 .getMatchedResourceMethod(),
                                                                            UnitOfWorkEventListener::registerUnitOfWorkAnnotations);

                // get property whether to have unitOfWork with DB default transaction by default
                useTransactionForUnitOfWork =
                        unitOfWork.isPresent() ? unitOfWork.get().transactional() : runWithTxnIfNotConfigured;
                TransactionIsolation transactionIsolation = unitOfWork.map(UnitOfWork::transactionIsolation)
                                                                      .orElse(TransactionIsolation.DEFAULT);
                if (useTransactionForUnitOfWork) {
                    transactionManager.beginTransaction(transactionIsolation);
                    isTransactionActive = true;
                }
            } else if (eventType == RequestEvent.Type.RESP_FILTERS_START) {
                // not supporting transactions to filters
            } else if (eventType == RequestEvent.Type.ON_EXCEPTION) {
                if (useTransactionForUnitOfWork && isTransactionActive) {
                    transactionManager.rollbackTransaction();
                    isTransactionActive = false;
                }
            } else if (eventType == RequestEvent.Type.FINISHED) {
                if (useTransactionForUnitOfWork && event.isSuccess()) {
                    transactionManager.commitTransaction();
                    isTransactionActive = false;
                }
                else if (useTransactionForUnitOfWork && !event.isSuccess() && isTransactionActive) {
                    transactionManager.rollbackTransaction();
                    isTransactionActive = false;
                }
            }
        }

        private static Optional<UnitOfWork> registerUnitOfWorkAnnotations(ResourceMethod method) {
            UnitOfWork annotation = method.getInvocable().getDefinitionMethod().getAnnotation(UnitOfWork.class);
            if (annotation == null) {
                annotation = method.getInvocable().getHandlingMethod().getAnnotation(UnitOfWork.class);
            }
            return Optional.ofNullable(annotation);
        }
    }

    @Override
    public void onEvent(ApplicationEvent applicationEvent) {
    }

    @Override
    public RequestEventListener onRequest(RequestEvent requestEvent) {
        return new UnitOfWorkEventListener(methodMap, transactionManager, runWithTxnIfNotConfigured);
    }
}