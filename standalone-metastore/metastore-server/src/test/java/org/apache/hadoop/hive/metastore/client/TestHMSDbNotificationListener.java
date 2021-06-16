package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class TestHMSDbNotificationListener extends TransactionalMetaStoreEventListener {
    public static boolean throwException = false;
    public TestHMSDbNotificationListener(Configuration config) throws MetaException {
        super(config);
    }

    @Override
    public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
        if (throwException) {
            throw new MetaException("Add partition failed in onAddPartition");
        }
    }
}
