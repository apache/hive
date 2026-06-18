package org.apache.hadoop.hive.metastore.metastore.iface;

import org.apache.hadoop.hive.metastore.metastore.MetaDescriptor;
import org.apache.hadoop.hive.metastore.metastore.impl.ConstraintStoreImpl;

@MetaDescriptor(alias = "constraint", defaultImpl = ConstraintStoreImpl.class)
public interface ConstraintStore {
}
