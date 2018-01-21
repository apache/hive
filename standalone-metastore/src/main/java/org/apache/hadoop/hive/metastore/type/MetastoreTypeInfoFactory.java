package org.apache.hadoop.hive.metastore.type;

import org.apache.hadoop.hive.metastore.ColumnType;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MetastoreTypeInfoFactory {
  // Map from type name (such as int or varchar(40) to the corresponding PrimitiveTypeInfo
  // instance.
  private static ConcurrentHashMap<String, MetastoreTypeInfo> cachedPrimitiveTypeInfo =
      new ConcurrentHashMap<String, MetastoreTypeInfo>();

  //non-parameterized primitive types
  private static final MetastoreTypeInfo voidTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.VOID_TYPE_NAME);
  private static final MetastoreTypeInfo booleanTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.BOOLEAN_TYPE_NAME);
  private static final MetastoreTypeInfo intTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.INT_TYPE_NAME);
  private static final MetastoreTypeInfo longTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.BIGINT_TYPE_NAME);
  private static final MetastoreTypeInfo stringTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.STRING_TYPE_NAME);
  private static final MetastoreTypeInfo floatTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.FLOAT_TYPE_NAME);
  private static final MetastoreTypeInfo doubleTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.DOUBLE_TYPE_NAME);
  private static final MetastoreTypeInfo byteTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.TINYINT_TYPE_NAME);
  private static final MetastoreTypeInfo shortTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.SMALLINT_TYPE_NAME);

  //date and time primitives
  private static final MetastoreTypeInfo dateTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.DATE_TYPE_NAME);
  private static final MetastoreTypeInfo timestampTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.TIMESTAMP_TYPE_NAME);
  /**
   * A TimestampTZTypeInfo with system default time zone.
   */
  private static final MetastoreTypeInfo timestampLocalTZTypeInfo =
      new TimestampLocalTZMetastoreTypeInfo(ZoneId.systemDefault().getId());

  private static final MetastoreTypeInfo intervalYearMonthTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.INTERVAL_YEAR_MONTH_TYPE_NAME);
  private static final MetastoreTypeInfo intervalDayTimeTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.INTERVAL_DAY_TIME_TYPE_NAME);
  private static final MetastoreTypeInfo binaryTypeInfo =
      new PrimitiveMetastoreTypeInfo(ColumnType.BINARY_TYPE_NAME);

  /**
   * A DecimalTypeInfo instance that has max precision and max scale.
   */
  //parameterized type infos
  private static final MetastoreTypeInfo decimalTypeInfo =
      new DecimalMetastoreTypeInfo(ColumnType.SYSTEM_DEFAULT_PRECISION,
          ColumnType.SYSTEM_DEFAULT_SCALE);

  private static final MetastoreTypeInfo charTypeInfo =
      new CharMetastoreTypeInfo(ColumnType.MAX_CHAR_LENGTH);

  private static final MetastoreTypeInfo varcharTypeInfo =
      new VarcharMetastoreTypeInfo(ColumnType.MAX_VARCHAR_LENGTH);

  static {
    cachedPrimitiveTypeInfo.put(ColumnType.VOID_TYPE_NAME, voidTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.BOOLEAN_TYPE_NAME, booleanTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.INT_TYPE_NAME, intTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.BIGINT_TYPE_NAME, longTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.STRING_TYPE_NAME, stringTypeInfo);
    cachedPrimitiveTypeInfo.put(charTypeInfo.getQualifiedName(), charTypeInfo);
    cachedPrimitiveTypeInfo.put(varcharTypeInfo.getQualifiedName(), varcharTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.FLOAT_TYPE_NAME, floatTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.DOUBLE_TYPE_NAME, doubleTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.TINYINT_TYPE_NAME, byteTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.SMALLINT_TYPE_NAME, shortTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.DATE_TYPE_NAME, dateTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.TIMESTAMP_TYPE_NAME, timestampTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.TIMESTAMPLOCALTZ_TYPE_NAME, timestampLocalTZTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.INTERVAL_YEAR_MONTH_TYPE_NAME, intervalYearMonthTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.INTERVAL_DAY_TIME_TYPE_NAME, intervalDayTimeTypeInfo);
    cachedPrimitiveTypeInfo.put(ColumnType.BINARY_TYPE_NAME, binaryTypeInfo);
    cachedPrimitiveTypeInfo.put(decimalTypeInfo.getQualifiedName(), decimalTypeInfo);
  }

  /**
   * Get PrimitiveTypeInfo instance for the given type name of a type
   * including types with parameters, such as varchar(20).
   *
   * @param typeName type name possibly with parameters.
   * @return aPrimitiveTypeInfo instance
   */
  public static MetastoreTypeInfo getPrimitiveTypeInfo(String typeName) {
    MetastoreTypeInfo result = cachedPrimitiveTypeInfo.get(typeName);
    if (result != null) {
      return result;
    }

    // Not found in the cache. Must be parameterized types. Create it.
    result = createPrimitiveMetastoreTypeInfo(typeName);
    if (result == null) {
      throw new RuntimeException("Error creating PrimitiveTypeInfo instance for " + typeName);
    }

    MetastoreTypeInfo prev = cachedPrimitiveTypeInfo.putIfAbsent(typeName, result);
    if (prev != null) {
      result = prev;
    }
    return result;
  }

  private static String getBaseName(String typeName) {
    int idx = typeName.indexOf('(');
    if (idx == -1) {
      return typeName;
    } else {
      return typeName.substring(0, idx);
    }
  }
  /**
   * Create PrimitiveTypeInfo instance for the given full name of the type. The returned
   * type is one of the parameterized type info such as VarcharTypeInfo.
   *
   * @param fullName Fully qualified name of the type
   * @return PrimitiveTypeInfo instance
   */
  private static PrimitiveMetastoreTypeInfo createPrimitiveMetastoreTypeInfo(String fullName) {
    String baseName = getBaseName(fullName);
    if (!ColumnType.AllTypes.contains(baseName)) {
      throw new RuntimeException("Unknown type " + fullName);
    }

    MetastoreTypeInfoParser.PrimitiveParts parts =
        new MetastoreTypeInfoParser(fullName).parsePrimitiveParts();
    if (parts.typeParams == null || parts.typeParams.length < 1) {
      return null;
    }

    switch (MetastorePrimitiveTypeCategory.from(baseName)) {
    case CHAR:
      if (parts.typeParams.length != 1) {
        return null;
      }
      return new CharMetastoreTypeInfo(Integer.valueOf(parts.typeParams[0]));
    case VARCHAR:
      if (parts.typeParams.length != 1) {
        return null;
      }
      return new VarcharMetastoreTypeInfo(Integer.valueOf(parts.typeParams[0]));
    case DECIMAL:
      if (parts.typeParams.length != 2) {
        return null;
      }
      return new DecimalMetastoreTypeInfo(Integer.valueOf(parts.typeParams[0]),
          Integer.valueOf(parts.typeParams[1]));
    case TIMESTAMPLOCALTZ:
      if (parts.typeParams.length != 1) {
        return null;
      }
      return new TimestampLocalTZMetastoreTypeInfo(parts.typeParams[0]);
    default:
      return null;
    }
  }

  static ConcurrentHashMap<ArrayList<List<?>>, MetastoreTypeInfo> cachedStructTypeInfo =
      new ConcurrentHashMap<>();

  public static MetastoreTypeInfo getStructTypeInfo(List<String> names,
      List<MetastoreTypeInfo> typeInfos) {
    ArrayList<List<?>> signature = new ArrayList<List<?>>(2);
    signature.add(names);
    signature.add(typeInfos);
    MetastoreTypeInfo result = cachedStructTypeInfo.get(signature);
    if (result == null) {
      result = new StructMetastoreTypeInfo(names, typeInfos);
      MetastoreTypeInfo prev = cachedStructTypeInfo.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<List<?>, MetastoreTypeInfo> cachedUnionTypeInfo =
      new ConcurrentHashMap<>();

  public static MetastoreTypeInfo getUnionTypeInfo(List<MetastoreTypeInfo> typeInfos) {
    MetastoreTypeInfo result = cachedUnionTypeInfo.get(typeInfos);
    if (result == null) {
      result = new UnionMetastoreTypeInfo(typeInfos);
      MetastoreTypeInfo prev = cachedUnionTypeInfo.putIfAbsent(typeInfos, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<MetastoreTypeInfo, MetastoreTypeInfo> cachedListTypeInfo =
      new ConcurrentHashMap<>();

  public static MetastoreTypeInfo getListTypeInfo(MetastoreTypeInfo elementTypeInfo) {
    MetastoreTypeInfo result = cachedListTypeInfo.get(elementTypeInfo);
    if (result == null) {
      result = new ListMetastoreTypeInfo(elementTypeInfo);
      MetastoreTypeInfo prev = cachedListTypeInfo.putIfAbsent(elementTypeInfo, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<ArrayList<MetastoreTypeInfo>, MetastoreTypeInfo> cachedMapTypeInfo =
      new ConcurrentHashMap<>();

  public static MetastoreTypeInfo getMapTypeInfo(MetastoreTypeInfo keyTypeInfo,
      MetastoreTypeInfo valueTypeInfo) {
    ArrayList <MetastoreTypeInfo> signature = new ArrayList <MetastoreTypeInfo>(2);
    signature.add(keyTypeInfo);
    signature.add(valueTypeInfo);
    MetastoreTypeInfo result = cachedMapTypeInfo.get(signature);
    if (result == null) {
      result = new MapMetastoreTypeInfo(keyTypeInfo, valueTypeInfo);
      MetastoreTypeInfo prev = cachedMapTypeInfo.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  public static CharMetastoreTypeInfo getCharTypeInfo(int length) {
    String fullName = CharMetastoreTypeInfo.getQualifiedName(ColumnType.CHAR_TYPE_NAME, length);
    return (CharMetastoreTypeInfo) getPrimitiveTypeInfo(fullName);
  }

  public static VarcharMetastoreTypeInfo getVarcharTypeInfo(int length) {
    String fullName =
        VarcharMetastoreTypeInfo.getQualifiedName(ColumnType.VARCHAR_TYPE_NAME, length);
    return (VarcharMetastoreTypeInfo) getPrimitiveTypeInfo(fullName);
  }

  public static DecimalMetastoreTypeInfo getDecimalTypeInfo(int precision, int scale) {
    String fullName = DecimalMetastoreTypeInfo.getQualifiedName(precision, scale);
    return (DecimalMetastoreTypeInfo) getPrimitiveTypeInfo(fullName);
  }

  public static TimestampLocalTZMetastoreTypeInfo getTimestampTZTypeInfo(ZoneId defaultTimeZone) {
    String fullName = TimestampLocalTZMetastoreTypeInfo.getQualifiedName(defaultTimeZone);
    return (TimestampLocalTZMetastoreTypeInfo) getPrimitiveTypeInfo(fullName);
  }
}
