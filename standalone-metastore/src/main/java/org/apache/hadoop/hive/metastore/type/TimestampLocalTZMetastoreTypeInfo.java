package org.apache.hadoop.hive.metastore.type;

import org.apache.hadoop.hive.metastore.ColumnType;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Objects;

public class TimestampLocalTZMetastoreTypeInfo extends PrimitiveMetastoreTypeInfo {
  private static final long serialVersionUID = 1L;

  private ZoneId timeZone;

  public TimestampLocalTZMetastoreTypeInfo() {
    super(ColumnType.TIMESTAMPLOCALTZ_TYPE_NAME);
  }

  public TimestampLocalTZMetastoreTypeInfo(String timeZoneStr) {
    super(ColumnType.TIMESTAMPLOCALTZ_TYPE_NAME);
    this.timeZone = parseTimeZone(timeZoneStr);
  }

  @Override
  public String getTypeName() {
    return ColumnType.TIMESTAMPLOCALTZ_TYPE_NAME;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    TimestampLocalTZMetastoreTypeInfo dti = (TimestampLocalTZMetastoreTypeInfo) other;

    return this.timeZone().equals(dti.timeZone());
  }

  /**
   * Generate the hashCode for this TypeInfo.
   */
  @Override
  public int hashCode() {
    return Objects.hash(typeName, timeZone);
  }

  @Override
  public String toString() {
    return getQualifiedName();
  }

  @Override
  public String getQualifiedName() {
    return getQualifiedName(timeZone);
  }

  public static String getQualifiedName(ZoneId timeZone) {
    StringBuilder sb = new StringBuilder(ColumnType.TIMESTAMPLOCALTZ_TYPE_NAME);
    sb.append("('");
    sb.append(timeZone);
    sb.append("')");
    return sb.toString();
  }

  public ZoneId timeZone() {
    return timeZone;
  }

  public ZoneId getTimeZone() {
    return timeZone;
  }

  public void setTimeZone(ZoneId timeZone) {
    this.timeZone = timeZone;
  }

  public static ZoneId parseTimeZone(String timeZoneStr) {
    if (timeZoneStr == null || timeZoneStr.trim().isEmpty() ||
        timeZoneStr.trim().toLowerCase().equals("local")) {
      // default
      return ZoneId.systemDefault();
    }
    try {
      return ZoneId.of(timeZoneStr);
    } catch (DateTimeException e1) {
      // default
      throw new RuntimeException("Invalid time zone displacement value");
    }
  }
}
