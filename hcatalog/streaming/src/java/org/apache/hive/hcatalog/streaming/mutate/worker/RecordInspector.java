package org.apache.hive.hcatalog.streaming.mutate.worker;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;

/** Provide a means to extract {@link RecordIdentifier} from record objects. */
public interface RecordInspector {

  /** Get the {@link RecordIdentifier} from the record - to be used for updates and deletes only. */
  RecordIdentifier extractRecordIdentifier(Object record);

}
