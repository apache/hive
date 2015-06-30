package org.apache.hive.hcatalog.streaming.mutate.worker;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * Interface for submitting mutation events to a given partition and bucket in an ACID table. Requires records to arrive
 * in the order defined by the {@link SequenceValidator}.
 */
public interface Mutator extends Closeable, Flushable {

  void insert(Object record) throws IOException;

  void update(Object record) throws IOException;

  void delete(Object record) throws IOException;

  void flush() throws IOException;

}
