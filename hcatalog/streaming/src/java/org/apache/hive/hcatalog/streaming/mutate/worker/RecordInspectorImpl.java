package org.apache.hive.hcatalog.streaming.mutate.worker;

import java.util.List;

import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Standard {@link RecordInspector} implementation that uses the supplied {@link ObjectInspector} and
 * {@link AcidOutputFormat.Options#recordIdColumn(int) record id column} to extract {@link RecordIdentifier
 * RecordIdentifiers}, and calculate bucket ids from records.
 */
public class RecordInspectorImpl implements RecordInspector {

  private final StructObjectInspector structObjectInspector;
  private final StructField recordIdentifierField;

  /**
   * Note that all column indexes are with respect to your record structure, not the Hive table structure.
   */
  public RecordInspectorImpl(ObjectInspector objectInspector, int recordIdColumn) {
    if (!(objectInspector instanceof StructObjectInspector)) {
      throw new IllegalArgumentException("Serious problem, expected a StructObjectInspector, " + "but got a "
          + objectInspector.getClass().getName());
    }

    structObjectInspector = (StructObjectInspector) objectInspector;
    List<? extends StructField> structFields = structObjectInspector.getAllStructFieldRefs();
    recordIdentifierField = structFields.get(recordIdColumn);
  }

  public RecordIdentifier extractRecordIdentifier(Object record) {
    return (RecordIdentifier) structObjectInspector.getStructFieldData(record, recordIdentifierField);
  }

  @Override
  public String toString() {
    return "RecordInspectorImpl [structObjectInspector=" + structObjectInspector + ", recordIdentifierField="
        + recordIdentifierField + "]";
  }

}
