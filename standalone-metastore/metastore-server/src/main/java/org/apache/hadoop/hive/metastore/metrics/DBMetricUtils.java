package org.apache.hadoop.hive.metastore.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.datanucleus.management.FactoryStatistics;

import java.util.SortedMap;

import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.*;

public class DBMetricUtils {
    public static void register(final FactoryStatistics dbStats) {
        MetricRegistry metrics = Metrics.getRegistry();
        if (metrics == null || dbStats == null) {
            return;
        }
        SortedMap<String, Gauge> gauges = metrics.getGauges();
        metrics.register(DB_NUMBER_OF_DATASTORE_READS, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getNumberOfDatastoreReads();
            }
        });
        metrics.register(DB_NUMBER_OF_DATASTORE_WRITES, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getNumberOfDatastoreWrites();
            }
        });
        metrics.register(DB_NUMBER_OF_DATASTORE_READS_IN_LATES_TXN, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getNumberOfDatastoreReadsInLatestTxn();
            }
        });
        metrics.register(DB_NUMBER_OF_DATASTORE_WRITES_IN_LATEST_TXN, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getNumberOfDatastoreWritesInLatestTxn();
            }
        });
        metrics.register(DB_NUMBER_OF_OBJECT_DELETES, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getNumberOfObjectDeletes();
            }
        });
        metrics.register(DB_NUMBER_OF_OBJECTS_FETCHES, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getNumberOfObjectFetches();
            }
        });
        metrics.register(DB_NUMBER_OF_OBJECT_INSERT, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getNumberOfObjectInserts();
            }
        });
        metrics.register(DB_NUMBER_OF_OBJECT_UPDATES, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getNumberOfObjectUpdates();
            }
        });
        metrics.register(DB_CONNECTION_ACTIVE_CURRENT, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getConnectionActiveCurrent();
            }
        });
        metrics.register(DB_CONNECTION_ACTIVE_TOTAL, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getConnectionActiveTotal();
            }
        });
        metrics.register(DB_CONNECTION_ACTIVE_HIGH, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getConnectionActiveHigh();
            }
        });
        metrics.register(DB_QUERY_EXECUTION_TIME_AVERAGE, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return dbStats.getQueryExecutionTimeAverage();
            }
        });
        metrics.register(DB_QUERY_EXECUTION_TIME_HIGH, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return dbStats.getQueryExecutionTimeHigh();
            }
        });
        metrics.register(DB_QUERY_ACTIVE_TOTAL_COUNT, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getQueryActiveTotalCount();
            }
        });
        metrics.register(DB_QUERY_ERROR_TOTAL_COUNT, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getQueryErrorTotalCount();
            }
        });
        metrics.register(DB_QUERY_EXECUTION_TIME_LOW, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return dbStats.getQueryExecutionTimeLow();
            }
        });
        metrics.register(DB_QUERY_EXECUTIO_TOTAL_COUNT, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getQueryExecutionTotalCount();
            }
        });
        metrics.register(DB_QUERY_EXECUTION_TOTAL_TIME, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return dbStats.getQueryExecutionTotalTime();
            }
        });
        metrics.register(DB_TRANSACTION_EXECUTION_TIME_AVERAGE, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return dbStats.getTransactionExecutionTimeAverage();
            }
        });
        metrics.register(DB_TRANSACTION_EXECUTION_TIME_HIGH, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return dbStats.getTransactionExecutionTimeHigh();
            }
        });
        metrics.register(DB_TRANSACTION_ACTIVE_TOTAL_COUNT, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getTransactionActiveTotalCount();
            }
        });
        metrics.register(DB_TRANSACTION_COMMITTED_TOTAL_COUNT, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getTransactionCommittedTotalCount();
            }
        });
        metrics.register(DB_TRANSACTION_EXECUTION_TIME_LOW, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return dbStats.getTransactionExecutionTimeLow();
            }
        });
        metrics.register(DB_TRANSACTION_EXECUTION_TOTAL_TIME, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return dbStats.getTransactionExecutionTotalTime();
            }
        });
        metrics.register(DB_TRANSACTION_ROLLEDBACK_TOTAL_COUNT, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getTransactionRolledBackTotalCount();
            }
        });
        metrics.register(DB_TRANSACTION_TOTAL_COUNT, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return dbStats.getTransactionTotalCount();
            }
        });
    }
}
