/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class ColumnStatistics implements org.apache.thrift.TBase<ColumnStatistics, ColumnStatistics._Fields>, java.io.Serializable, Cloneable, Comparable<ColumnStatistics> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ColumnStatistics");

  private static final org.apache.thrift.protocol.TField STATS_DESC_FIELD_DESC = new org.apache.thrift.protocol.TField("statsDesc", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField STATS_OBJ_FIELD_DESC = new org.apache.thrift.protocol.TField("statsObj", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ColumnStatisticsStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ColumnStatisticsTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable ColumnStatisticsDesc statsDesc; // required
  private @org.apache.thrift.annotation.Nullable java.util.List<ColumnStatisticsObj> statsObj; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STATS_DESC((short)1, "statsDesc"),
    STATS_OBJ((short)2, "statsObj");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // STATS_DESC
          return STATS_DESC;
        case 2: // STATS_OBJ
          return STATS_OBJ;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STATS_DESC, new org.apache.thrift.meta_data.FieldMetaData("statsDesc", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ColumnStatisticsDesc.class)));
    tmpMap.put(_Fields.STATS_OBJ, new org.apache.thrift.meta_data.FieldMetaData("statsObj", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ColumnStatisticsObj.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ColumnStatistics.class, metaDataMap);
  }

  public ColumnStatistics() {
  }

  public ColumnStatistics(
    ColumnStatisticsDesc statsDesc,
    java.util.List<ColumnStatisticsObj> statsObj)
  {
    this();
    this.statsDesc = statsDesc;
    this.statsObj = statsObj;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ColumnStatistics(ColumnStatistics other) {
    if (other.isSetStatsDesc()) {
      this.statsDesc = new ColumnStatisticsDesc(other.statsDesc);
    }
    if (other.isSetStatsObj()) {
      java.util.List<ColumnStatisticsObj> __this__statsObj = new java.util.ArrayList<ColumnStatisticsObj>(other.statsObj.size());
      for (ColumnStatisticsObj other_element : other.statsObj) {
        __this__statsObj.add(new ColumnStatisticsObj(other_element));
      }
      this.statsObj = __this__statsObj;
    }
  }

  public ColumnStatistics deepCopy() {
    return new ColumnStatistics(this);
  }

  @Override
  public void clear() {
    this.statsDesc = null;
    this.statsObj = null;
  }

  @org.apache.thrift.annotation.Nullable
  public ColumnStatisticsDesc getStatsDesc() {
    return this.statsDesc;
  }

  public void setStatsDesc(@org.apache.thrift.annotation.Nullable ColumnStatisticsDesc statsDesc) {
    this.statsDesc = statsDesc;
  }

  public void unsetStatsDesc() {
    this.statsDesc = null;
  }

  /** Returns true if field statsDesc is set (has been assigned a value) and false otherwise */
  public boolean isSetStatsDesc() {
    return this.statsDesc != null;
  }

  public void setStatsDescIsSet(boolean value) {
    if (!value) {
      this.statsDesc = null;
    }
  }

  public int getStatsObjSize() {
    return (this.statsObj == null) ? 0 : this.statsObj.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<ColumnStatisticsObj> getStatsObjIterator() {
    return (this.statsObj == null) ? null : this.statsObj.iterator();
  }

  public void addToStatsObj(ColumnStatisticsObj elem) {
    if (this.statsObj == null) {
      this.statsObj = new java.util.ArrayList<ColumnStatisticsObj>();
    }
    this.statsObj.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<ColumnStatisticsObj> getStatsObj() {
    return this.statsObj;
  }

  public void setStatsObj(@org.apache.thrift.annotation.Nullable java.util.List<ColumnStatisticsObj> statsObj) {
    this.statsObj = statsObj;
  }

  public void unsetStatsObj() {
    this.statsObj = null;
  }

  /** Returns true if field statsObj is set (has been assigned a value) and false otherwise */
  public boolean isSetStatsObj() {
    return this.statsObj != null;
  }

  public void setStatsObjIsSet(boolean value) {
    if (!value) {
      this.statsObj = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case STATS_DESC:
      if (value == null) {
        unsetStatsDesc();
      } else {
        setStatsDesc((ColumnStatisticsDesc)value);
      }
      break;

    case STATS_OBJ:
      if (value == null) {
        unsetStatsObj();
      } else {
        setStatsObj((java.util.List<ColumnStatisticsObj>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case STATS_DESC:
      return getStatsDesc();

    case STATS_OBJ:
      return getStatsObj();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case STATS_DESC:
      return isSetStatsDesc();
    case STATS_OBJ:
      return isSetStatsObj();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof ColumnStatistics)
      return this.equals((ColumnStatistics)that);
    return false;
  }

  public boolean equals(ColumnStatistics that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_statsDesc = true && this.isSetStatsDesc();
    boolean that_present_statsDesc = true && that.isSetStatsDesc();
    if (this_present_statsDesc || that_present_statsDesc) {
      if (!(this_present_statsDesc && that_present_statsDesc))
        return false;
      if (!this.statsDesc.equals(that.statsDesc))
        return false;
    }

    boolean this_present_statsObj = true && this.isSetStatsObj();
    boolean that_present_statsObj = true && that.isSetStatsObj();
    if (this_present_statsObj || that_present_statsObj) {
      if (!(this_present_statsObj && that_present_statsObj))
        return false;
      if (!this.statsObj.equals(that.statsObj))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetStatsDesc()) ? 131071 : 524287);
    if (isSetStatsDesc())
      hashCode = hashCode * 8191 + statsDesc.hashCode();

    hashCode = hashCode * 8191 + ((isSetStatsObj()) ? 131071 : 524287);
    if (isSetStatsObj())
      hashCode = hashCode * 8191 + statsObj.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(ColumnStatistics other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetStatsDesc()).compareTo(other.isSetStatsDesc());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatsDesc()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.statsDesc, other.statsDesc);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetStatsObj()).compareTo(other.isSetStatsObj());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatsObj()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.statsObj, other.statsObj);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ColumnStatistics(");
    boolean first = true;

    sb.append("statsDesc:");
    if (this.statsDesc == null) {
      sb.append("null");
    } else {
      sb.append(this.statsDesc);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("statsObj:");
    if (this.statsObj == null) {
      sb.append("null");
    } else {
      sb.append(this.statsObj);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetStatsDesc()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'statsDesc' is unset! Struct:" + toString());
    }

    if (!isSetStatsObj()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'statsObj' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (statsDesc != null) {
      statsDesc.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ColumnStatisticsStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ColumnStatisticsStandardScheme getScheme() {
      return new ColumnStatisticsStandardScheme();
    }
  }

  private static class ColumnStatisticsStandardScheme extends org.apache.thrift.scheme.StandardScheme<ColumnStatistics> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ColumnStatistics struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // STATS_DESC
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.statsDesc = new ColumnStatisticsDesc();
              struct.statsDesc.read(iprot);
              struct.setStatsDescIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STATS_OBJ
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list276 = iprot.readListBegin();
                struct.statsObj = new java.util.ArrayList<ColumnStatisticsObj>(_list276.size);
                @org.apache.thrift.annotation.Nullable ColumnStatisticsObj _elem277;
                for (int _i278 = 0; _i278 < _list276.size; ++_i278)
                {
                  _elem277 = new ColumnStatisticsObj();
                  _elem277.read(iprot);
                  struct.statsObj.add(_elem277);
                }
                iprot.readListEnd();
              }
              struct.setStatsObjIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ColumnStatistics struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.statsDesc != null) {
        oprot.writeFieldBegin(STATS_DESC_FIELD_DESC);
        struct.statsDesc.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.statsObj != null) {
        oprot.writeFieldBegin(STATS_OBJ_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.statsObj.size()));
          for (ColumnStatisticsObj _iter279 : struct.statsObj)
          {
            _iter279.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ColumnStatisticsTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ColumnStatisticsTupleScheme getScheme() {
      return new ColumnStatisticsTupleScheme();
    }
  }

  private static class ColumnStatisticsTupleScheme extends org.apache.thrift.scheme.TupleScheme<ColumnStatistics> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ColumnStatistics struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.statsDesc.write(oprot);
      {
        oprot.writeI32(struct.statsObj.size());
        for (ColumnStatisticsObj _iter280 : struct.statsObj)
        {
          _iter280.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ColumnStatistics struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.statsDesc = new ColumnStatisticsDesc();
      struct.statsDesc.read(iprot);
      struct.setStatsDescIsSet(true);
      {
        org.apache.thrift.protocol.TList _list281 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.statsObj = new java.util.ArrayList<ColumnStatisticsObj>(_list281.size);
        @org.apache.thrift.annotation.Nullable ColumnStatisticsObj _elem282;
        for (int _i283 = 0; _i283 < _list281.size; ++_i283)
        {
          _elem282 = new ColumnStatisticsObj();
          _elem282.read(iprot);
          struct.statsObj.add(_elem282);
        }
      }
      struct.setStatsObjIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}
