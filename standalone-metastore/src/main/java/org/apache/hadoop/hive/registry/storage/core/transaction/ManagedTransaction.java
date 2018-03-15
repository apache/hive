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
import org.apache.hadoop.hive.registry.storage.core.TransactionManager;
import org.apache.hadoop.hive.registry.storage.core.transaction.functional.ManagedTransactionConsumer;
import org.apache.hadoop.hive.registry.storage.core.transaction.functional.ManagedTransactionFunction;
import org.apache.hadoop.hive.registry.storage.core.exception.IgnoreTransactionRollbackException;

/**
 * Utility class for providing managed transaction to make life easier.
 */
public class ManagedTransaction {
    private final TransactionManager transactionManager;
    private final TransactionIsolation transactionIsolation;

    /**
     * Constructor.
     *
     * @param transactionManager the instance of TransactionManager which manages transaction
     */
    public ManagedTransaction(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        this.transactionIsolation = TransactionIsolation.DEFAULT;
    }

    /**
     * Constructor.
     *
     * @param transactionManager the instance of TransactionManager which manages transaction
     * @param transactionIsolation isolation level for transaction
     */
    public ManagedTransaction(TransactionManager transactionManager, TransactionIsolation transactionIsolation) {
        this.transactionManager = transactionManager;
        this.transactionIsolation = transactionIsolation;
    }

    /**
     * Execute function with managed transaction.
     *
     * @param fn function to execute with managed transaction
     * @param <R> type for return value of function
     * @return the return value of function
     * @throws Exception
     */
    public <R> R executeFunction(ManagedTransactionFunction.Arg0<R> fn) throws Exception {
        return executeTransactionBlockInternal(() -> fn.apply());
    }

    /**
     * Execute function with managed transaction.
     *
     * @param fn function to execute with managed transaction
     * @param <T1> type for parameter1 of function
     * @param <R> type for return value of function
     * @return the return value of function
     * @throws Exception
     */
    public <T1, R> R executeFunction(ManagedTransactionFunction.Arg1<T1, R> fn, T1 t) throws Exception {
        return executeTransactionBlockInternal(() -> fn.apply(t));
    }

    /**
     * Execute function with managed transaction.
     *
     * @param fn function to execute with managed transaction
     * @param <T1> type for parameter1 of function
     * @param <T2> type for parameter2 of function
     * @param <R> type for return value of function
     * @return the return value of function
     * @throws Exception
     */
    public <T1, T2, R> R executeFunction(ManagedTransactionFunction.Arg2<T1, T2, R> fn, T1 t1, T2 t2) throws Exception {
        return executeTransactionBlockInternal(() -> fn.apply(t1, t2));
    }

    /**
     * Execute function with managed transaction.
     *
     * @param fn function to execute with managed transaction
     * @param <T1> type for parameter1 of function
     * @param <T2> type for parameter2 of function
     * @param <T3> type for parameter3 of function
     * @param <R> type for return value of function
     * @return the return value of function
     * @throws Exception
     */
    public <T1, T2, T3, R> R executeFunction(ManagedTransactionFunction.Arg3<T1, T2, T3, R> fn,
                                             T1 t1, T2 t2, T3 t3) throws Exception {
        return executeTransactionBlockInternal(() -> fn.apply(t1, t2, t3));
    }

    /**
     * Execute function with managed transaction.
     *
     * @param fn function to execute with managed transaction
     * @param <T1> type for parameter1 of function
     * @param <T2> type for parameter2 of function
     * @param <T3> type for parameter3 of function
     * @param <T4> type for parameter4 of function
     * @param <R> type for return value of function
     * @return the return value of function
     * @throws Exception
     */
    public <T1, T2, T3, T4, R> R executeFunction(ManagedTransactionFunction.Arg4<T1, T2, T3, T4, R> fn,
                                                 T1 t1, T2 t2, T3 t3, T4 t4) throws Exception {
        return executeTransactionBlockInternal(() -> fn.apply(t1, t2, t3, t4));
    }

    /**
     * Execute function with managed transaction.
     *
     * @param fn function to execute with managed transaction
     * @param <T1> type for parameter1 of function
     * @param <T2> type for parameter2 of function
     * @param <T3> type for parameter3 of function
     * @param <T4> type for parameter4 of function
     * @param <T5> type for parameter5 of function
     * @param <R> type for return value of function
     * @return the return value of function
     * @throws Exception
     */
    public <T1, T2, T3, T4, T5, R> R executeFunction(ManagedTransactionFunction.Arg5<T1, T2, T3, T4, T5, R> fn,
                                                     T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) throws Exception {
        return executeTransactionBlockInternal(() -> fn.apply(t1, t2, t3, t4, t5));
    }

    /**
     * Execute consumer with managed transaction.
     *
     * @param fn consumer to execute with managed transaction
     * @throws Exception
     */
    public void executeConsumer(ManagedTransactionConsumer.Arg0 fn) throws Exception {
        executeTransactionBlockInternal(() -> {
            fn.apply();
            return null;
        });
    }

    /**
     * Execute consumer with managed transaction.
     *
     * @param fn consumer to execute with managed transaction
     * @param <T1> type for parameter1 of consumer
     * @throws Exception
     */
    public <T1> void executeConsumer(ManagedTransactionConsumer.Arg1<T1> fn, T1 t) throws Exception {
        executeTransactionBlockInternal(() -> {
            fn.apply(t);
            return null;
        });
    }

    /**
     * Execute consumer with managed transaction.
     *
     * @param fn consumer to execute with managed transaction
     * @param <T1> type for parameter1 of consumer
     * @param <T2> type for parameter2 of consumer
     * @throws Exception
     */
    public <T1, T2> void executeConsumer(ManagedTransactionConsumer.Arg2<T1, T2> fn, T1 t1, T2 t2) throws Exception {
        executeTransactionBlockInternal(() -> {
            fn.apply(t1, t2);
            return null;
        });
    }

    /**
     * Execute consumer with managed transaction.
     *
     * @param fn consumer to execute with managed transaction
     * @param <T1> type for parameter1 of consumer
     * @param <T2> type for parameter2 of consumer
     * @param <T3> type for parameter3 of consumer
     * @throws Exception
     */
    public <T1, T2, T3> void executeConsumer(ManagedTransactionConsumer.Arg3<T1, T2, T3> fn,
                                             T1 t1, T2 t2, T3 t3) throws Exception {
        executeTransactionBlockInternal(() -> {
            fn.apply(t1, t2, t3);
            return null;
        });
    }

    /**
     * Execute consumer with managed transaction.
     *
     * @param fn consumer to execute with managed transaction
     * @param <T1> type for parameter1 of consumer
     * @param <T2> type for parameter2 of consumer
     * @param <T3> type for parameter3 of consumer
     * @param <T4> type for parameter4 of consumer
     * @throws Exception
     */
    public <T1, T2, T3, T4> void executeConsumer(ManagedTransactionConsumer.Arg4<T1, T2, T3, T4> fn,
                                                 T1 t1, T2 t2, T3 t3, T4 t4) throws Exception {
        executeTransactionBlockInternal(() -> {
            fn.apply(t1, t2, t3, t4);
            return null;
        });
    }

    /**
     * Execute consumer with managed transaction.
     *
     * @param fn consumer to execute with managed transaction
     * @param <T1> type for parameter1 of consumer
     * @param <T2> type for parameter2 of consumer
     * @param <T3> type for parameter3 of consumer
     * @param <T4> type for parameter4 of consumer
     * @param <T5> type for parameter5 of consumer
     * @throws Exception
     */
    public <T1, T2, T3, T4, T5> void executeConsumer(ManagedTransactionConsumer.Arg5<T1, T2, T3, T4, T5> fn,
                                                     T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) throws Exception {
        executeTransactionBlockInternal(() -> {
            fn.apply(t1, t2, t3, t4, t5);
            return null;
        });
    }

    private <R> R executeTransactionBlockInternal(SupplierCapableOfThrowingException<R> fn) throws Exception {
        boolean committed = false;
        try {
            transactionManager.beginTransaction(transactionIsolation);
            R r = fn.get();
            transactionManager.commitTransaction();
            committed = true;
            return r;
        } catch (IgnoreTransactionRollbackException e) {
            transactionManager.commitTransaction();
            committed = true;
            throw convertThrowableToException(e.getCause());
        } finally {
            if (!committed) {
                transactionManager.rollbackTransaction();
            }
        }
    }

    private Exception convertThrowableToException(Throwable t) {
        if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof Exception) {
            return (Exception) t;
        } else {
            return new RuntimeException(t);
        }
    }

    private interface SupplierCapableOfThrowingException<R> {
        R get() throws Exception;
    }

}
