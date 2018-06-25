/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.serde2;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.common.util.DateUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;

import com.google.common.base.Preconditions;
import com.google.common.base.Charsets;

/**
 * Generate object inspector and random row object[].
 */
public class SerdeRandomRowSource {

  private Random r;

  private int columnCount;

  private List<String> typeNames;

  private Category[] categories;

  private TypeInfo[] typeInfos;

  private List<ObjectInspector> objectInspectorList;

  // Primitive.

  private PrimitiveCategory[] primitiveCategories;

  private PrimitiveTypeInfo[] primitiveTypeInfos;

  private List<ObjectInspector> primitiveObjectInspectorList;

  private StructObjectInspector rowStructObjectInspector;

  private String[] alphabets;

  private boolean allowNull;

  private boolean addEscapables;
  private String needsEscapeStr;

  public List<String> typeNames() {
    return typeNames;
  }

  public Category[] categories() {
    return categories;
  }

  public TypeInfo[] typeInfos() {
    return typeInfos;
  }

  public PrimitiveCategory[] primitiveCategories() {
    return primitiveCategories;
  }

  public PrimitiveTypeInfo[] primitiveTypeInfos() {
    return primitiveTypeInfos;
  }

  public StructObjectInspector rowStructObjectInspector() {
    return rowStructObjectInspector;
  }

  public StructObjectInspector partialRowStructObjectInspector(int partialFieldCount) {
    ArrayList<ObjectInspector> partialObjectInspectorList =
        new ArrayList<ObjectInspector>(partialFieldCount);
    List<String> columnNames = new ArrayList<String>(partialFieldCount);
    for (int i = 0; i < partialFieldCount; i++) {
      columnNames.add(String.format("partial%d", i));
      partialObjectInspectorList.add(getObjectInspector(typeInfos[i]));
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, objectInspectorList);
  }

  public enum SupportedTypes {
    ALL, PRIMITIVE, ALL_EXCEPT_MAP
  }

  public void init(Random r, SupportedTypes supportedTypes, int maxComplexDepth) {
    init(r, supportedTypes, maxComplexDepth, true);
  }

  public void init(Random r, SupportedTypes supportedTypes, int maxComplexDepth, boolean allowNull) {
    this.r = r;
    this.allowNull = allowNull;
    chooseSchema(supportedTypes, maxComplexDepth);
  }

  /*
   * For now, exclude CHAR until we determine why there is a difference (blank padding)
   * serializing with LazyBinarySerializeWrite and the regular SerDe...
   */
  private static String[] possibleHivePrimitiveTypeNames = {
      "boolean",
      "tinyint",
      "smallint",
      "int",
      "bigint",
      "date",
      "float",
      "double",
      "string",
//    "char",
      "varchar",
      "binary",
      "date",
      "timestamp",
      "interval_year_month",
      "interval_day_time",
      "decimal"
  };

  private static String[] possibleHiveComplexTypeNames = {
    "array",
    "struct",
    "uniontype",
    "map"
  };

  private String getRandomTypeName(SupportedTypes supportedTypes) {
    String typeName = null;
    if (r.nextInt(10 ) != 0) {
      typeName = possibleHivePrimitiveTypeNames[r.nextInt(possibleHivePrimitiveTypeNames.length)];
    } else {
      switch (supportedTypes) {
      case PRIMITIVE:
        typeName = possibleHivePrimitiveTypeNames[r.nextInt(possibleHivePrimitiveTypeNames.length)];
        break;
      case ALL_EXCEPT_MAP:
        typeName = possibleHiveComplexTypeNames[r.nextInt(possibleHiveComplexTypeNames.length - 1)];
        break;
      case ALL:
        typeName = possibleHiveComplexTypeNames[r.nextInt(possibleHiveComplexTypeNames.length)];
        break;
      }
    }
    return typeName;
  }

  private String getDecoratedTypeName(String typeName, SupportedTypes supportedTypes, int depth, int maxDepth) {
    depth++;
    if (depth < maxDepth) {
      supportedTypes = SupportedTypes.PRIMITIVE;
    }
    if (typeName.equals("char")) {
      int maxLength = 1 + r.nextInt(100);
      typeName = String.format("char(%d)", maxLength);
    } else if (typeName.equals("varchar")) {
      int maxLength = 1 + r.nextInt(100);
      typeName = String.format("varchar(%d)", maxLength);
    } else if (typeName.equals("decimal")) {
      typeName = String.format("decimal(%d,%d)", HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);
    } else if (typeName.equals("array")) {
      String elementTypeName = getRandomTypeName(supportedTypes);
      elementTypeName = getDecoratedTypeName(elementTypeName, supportedTypes, depth, maxDepth);
      typeName = String.format("array<%s>", elementTypeName);
    } else if (typeName.equals("map")) {
      String keyTypeName = getRandomTypeName(SupportedTypes.PRIMITIVE);
      keyTypeName = getDecoratedTypeName(keyTypeName, supportedTypes, depth, maxDepth);
      String valueTypeName = getRandomTypeName(supportedTypes);
      valueTypeName = getDecoratedTypeName(valueTypeName, supportedTypes, depth, maxDepth);
      typeName = String.format("map<%s,%s>", keyTypeName, valueTypeName);
    } else if (typeName.equals("struct")) {
      final int fieldCount = 1 + r.nextInt(10);
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fieldCount; i++) {
        String fieldTypeName = getRandomTypeName(supportedTypes);
        fieldTypeName = getDecoratedTypeName(fieldTypeName, supportedTypes, depth, maxDepth);
        if (i > 0) {
          sb.append(",");
        }
        sb.append("col");
        sb.append(i);
        sb.append(":");
        sb.append(fieldTypeName);
      }
      typeName = String.format("struct<%s>", sb.toString());
    } else if (typeName.equals("struct") ||
      typeName.equals("uniontype")) {
      final int fieldCount = 1 + r.nextInt(10);
      final StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fieldCount; i++) {
      String fieldTypeName = getRandomTypeName(supportedTypes);
      fieldTypeName = getDecoratedTypeName(fieldTypeName, supportedTypes, depth, maxDepth);
      if (i > 0) {
        sb.append(",");
      }
      sb.append(fieldTypeName);
      }
      typeName = String.format("uniontype<%s>", sb.toString());
    }
    return typeName;
  }

  private ObjectInspector getObjectInspector(TypeInfo typeInfo) {
    ObjectInspector objectInspector;
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      {
        final PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) typeInfo;
        objectInspector =
            PrimitiveObjectInspectorFactory.
                getPrimitiveWritableObjectInspector(primitiveType);
      }
      break;
    case MAP:
      {
        final MapTypeInfo mapType = (MapTypeInfo) typeInfo;
        final MapObjectInspector mapInspector =
            ObjectInspectorFactory.getStandardMapObjectInspector(
                getObjectInspector(mapType.getMapKeyTypeInfo()),
                getObjectInspector(mapType.getMapValueTypeInfo()));
        objectInspector = mapInspector;
      }
      break;
    case LIST:
      {
        final ListTypeInfo listType = (ListTypeInfo) typeInfo;
        final ListObjectInspector listInspector =
            ObjectInspectorFactory.getStandardListObjectInspector(
                getObjectInspector(listType.getListElementTypeInfo()));
        objectInspector = listInspector;
      }
      break;
    case STRUCT:
      {
        final StructTypeInfo structType = (StructTypeInfo) typeInfo;
        final List<TypeInfo> fieldTypes = structType.getAllStructFieldTypeInfos();

        final List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
        for (TypeInfo fieldType : fieldTypes) {
          fieldInspectors.add(getObjectInspector(fieldType));
        }

        final StructObjectInspector structInspector =
            ObjectInspectorFactory.getStandardStructObjectInspector(
                structType.getAllStructFieldNames(), fieldInspectors);
        objectInspector = structInspector;
      }
      break;
    case UNION:
      {
        final UnionTypeInfo unionType = (UnionTypeInfo) typeInfo;
        final List<TypeInfo> fieldTypes = unionType.getAllUnionObjectTypeInfos();

        final List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
        for (TypeInfo fieldType : fieldTypes) {
          fieldInspectors.add(getObjectInspector(fieldType));
        }

        final UnionObjectInspector unionInspector =
            ObjectInspectorFactory.getStandardUnionObjectInspector(
                fieldInspectors);
        objectInspector = unionInspector;
      }
      break;
    default:
      throw new RuntimeException("Unexpected category " + typeInfo.getCategory());
    }
    Preconditions.checkState(objectInspector != null);
    return objectInspector;
  }

  private void chooseSchema(SupportedTypes supportedTypes, int maxComplexDepth) {
    HashSet hashSet = null;
    final boolean allTypes;
    final boolean onlyOne = (r.nextInt(100) == 7);
    if (onlyOne) {
      columnCount = 1;
      allTypes = false;
    } else {
      allTypes = r.nextBoolean();
      if (allTypes) {
        switch (supportedTypes) {
        case ALL:
          columnCount = possibleHivePrimitiveTypeNames.length + possibleHiveComplexTypeNames.length;
          break;
        case ALL_EXCEPT_MAP:
          columnCount = possibleHivePrimitiveTypeNames.length + possibleHiveComplexTypeNames.length - 1;
          break;
        case PRIMITIVE:
          columnCount = possibleHivePrimitiveTypeNames.length;
          break;
        }
        hashSet = new HashSet<Integer>();
      } else {
        columnCount = 1 + r.nextInt(20);
      }
    }
    typeNames = new ArrayList<String>(columnCount);
    categories = new Category[columnCount];
    typeInfos = new TypeInfo[columnCount];
    objectInspectorList = new ArrayList<ObjectInspector>(columnCount);

    primitiveCategories = new PrimitiveCategory[columnCount];
    primitiveTypeInfos = new PrimitiveTypeInfo[columnCount];
    primitiveObjectInspectorList = new ArrayList<ObjectInspector>(columnCount);
    final List<String> columnNames = new ArrayList<String>(columnCount);
    for (int c = 0; c < columnCount; c++) {
      columnNames.add(String.format("col%d", c));
      String typeName;

      if (onlyOne) {
        typeName = getRandomTypeName(supportedTypes);
      } else {
        int typeNum;
        if (allTypes) {
          int maxTypeNum = 0;
          switch (supportedTypes) {
          case PRIMITIVE:
            maxTypeNum = possibleHivePrimitiveTypeNames.length;
            break;
          case ALL_EXCEPT_MAP:
            maxTypeNum = possibleHivePrimitiveTypeNames.length + possibleHiveComplexTypeNames.length - 1;
            break;
          case ALL:
            maxTypeNum = possibleHivePrimitiveTypeNames.length + possibleHiveComplexTypeNames.length;
            break;
          }
          while (true) {

            typeNum = r.nextInt(maxTypeNum);

            final Integer typeNumInteger = new Integer(typeNum);
            if (!hashSet.contains(typeNumInteger)) {
              hashSet.add(typeNumInteger);
              break;
            }
          }
        } else {
          if (supportedTypes == SupportedTypes.PRIMITIVE || r.nextInt(10) != 0) {
            typeNum = r.nextInt(possibleHivePrimitiveTypeNames.length);
          } else {
            typeNum = possibleHivePrimitiveTypeNames.length + r.nextInt(possibleHiveComplexTypeNames.length);
            if (supportedTypes == SupportedTypes.ALL_EXCEPT_MAP) {
              typeNum--;
            }
          }
        }
        if (typeNum < possibleHivePrimitiveTypeNames.length) {
          typeName = possibleHivePrimitiveTypeNames[typeNum];
        } else {
          typeName = possibleHiveComplexTypeNames[typeNum - possibleHivePrimitiveTypeNames.length];
        }
        
      }

      final String decoratedTypeName = getDecoratedTypeName(typeName, supportedTypes, 0, maxComplexDepth);

      final TypeInfo typeInfo;
      try {
        typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(decoratedTypeName);
      } catch (Exception e) {
        throw new RuntimeException("Cannot convert type name " + decoratedTypeName + " to a type " + e);
      }
      
      typeInfos[c] = typeInfo;
      final Category category = typeInfo.getCategory();
      categories[c] = category;
      ObjectInspector objectInspector = getObjectInspector(typeInfo);
      switch (category) {
      case PRIMITIVE:
        {
          final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
          final PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
          objectInspector = PrimitiveObjectInspectorFactory.
              getPrimitiveWritableObjectInspector(primitiveTypeInfo);
          primitiveTypeInfos[c] = primitiveTypeInfo;
          primitiveCategories[c] = primitiveCategory;
          primitiveObjectInspectorList.add(objectInspector);
        }
        break;
      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
        primitiveObjectInspectorList.add(null);
        break;
      default:
        throw new RuntimeException("Unexpected catagory " + category);
      }
      objectInspectorList.add(objectInspector);

      if (category == Category.PRIMITIVE) {
      }
      typeNames.add(decoratedTypeName);
    }
    rowStructObjectInspector = ObjectInspectorFactory.
        getStandardStructObjectInspector(columnNames, objectInspectorList);
    alphabets = new String[columnCount];
  }

  public Object[][] randomRows(int n) {
    Object[][] result = new Object[n][];
    for (int i = 0; i < n; i++) {
      result[i] = randomRow();
    }
    return result;
  }

  public Object[] randomRow() {
    Object row[] = new Object[columnCount];
    for (int c = 0; c < columnCount; c++) {
      row[c] = randomWritable(c);
    }
    return row;
  }

  public Object[] randomPrimitiveRow(int columnCount) {
    return randomPrimitiveRow(columnCount, r, primitiveTypeInfos);
  }

  public static Object[] randomPrimitiveRow(int columnCount, Random r,
      PrimitiveTypeInfo[] primitiveTypeInfos) {
    final Object row[] = new Object[columnCount];
    for (int c = 0; c < columnCount; c++) {
      row[c] = randomPrimitiveObject(r, primitiveTypeInfos[c]);
    }
    return row;
  }

  public static Object[] randomWritablePrimitiveRow(
      int columnCount, Random r, PrimitiveTypeInfo[] primitiveTypeInfos) {

    final Object row[] = new Object[columnCount];
    for (int c = 0; c < columnCount; c++) {
      final PrimitiveTypeInfo primitiveTypeInfo = primitiveTypeInfos[c];
      final ObjectInspector objectInspector =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(primitiveTypeInfo);
      final Object object = randomPrimitiveObject(r, primitiveTypeInfo);
      row[c] = getWritablePrimitiveObject(primitiveTypeInfo, objectInspector, object);
    }
    return row;
  }

  public void addBinarySortableAlphabets() {
    for (int c = 0; c < columnCount; c++) {
      switch (primitiveCategories[c]) {
      case STRING:
      case CHAR:
      case VARCHAR:
        byte[] bytes = new byte[10 + r.nextInt(10)];
        for (int i = 0; i < bytes.length; i++) {
          bytes[i] = (byte) (32 + r.nextInt(96));
        }
        final int alwaysIndex = r.nextInt(bytes.length);
        bytes[alwaysIndex] = 0;  // Must be escaped by BinarySortable.
        final int alwaysIndex2 = r.nextInt(bytes.length);
        bytes[alwaysIndex2] = 1;  // Must be escaped by BinarySortable.
        alphabets[c] = new String(bytes, Charsets.UTF_8);
        break;
      default:
        // No alphabet needed.
        break;
      }
    }
  }

  public void addEscapables(String needsEscapeStr) {
    addEscapables = true;
    this.needsEscapeStr = needsEscapeStr;
  }

  public static void sort(Object[][] rows, ObjectInspector oi) {
    for (int i = 0; i < rows.length; i++) {
      for (int j = i + 1; j < rows.length; j++) {
        if (ObjectInspectorUtils.compare(rows[i], oi, rows[j], oi) > 0) {
          final Object[] t = rows[i];
          rows[i] = rows[j];
          rows[j] = t;
        }
      }
    }
  }

  public void sort(Object[][] rows) {
    SerdeRandomRowSource.sort(rows, rowStructObjectInspector);
  }

  public static Object getWritablePrimitiveObject(
      PrimitiveTypeInfo primitiveTypeInfo, ObjectInspector objectInspector, Object object) {
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
    case BOOLEAN:
      return ((WritableBooleanObjectInspector) objectInspector).create((boolean) object);
    case BYTE:
      return ((WritableByteObjectInspector) objectInspector).create((byte) object);
    case SHORT:
      return ((WritableShortObjectInspector) objectInspector).create((short) object);
    case INT:
      return ((WritableIntObjectInspector) objectInspector).create((int) object);
    case LONG:
      return ((WritableLongObjectInspector) objectInspector).create((long) object);
    case DATE:
      return ((WritableDateObjectInspector) objectInspector).create((Date) object);
    case FLOAT:
      return ((WritableFloatObjectInspector) objectInspector).create((float) object);
    case DOUBLE:
      return ((WritableDoubleObjectInspector) objectInspector).create((double) object);
    case STRING:
      return ((WritableStringObjectInspector) objectInspector).create((String) object);
    case CHAR:
      {
        final WritableHiveCharObjectInspector writableCharObjectInspector =
                new WritableHiveCharObjectInspector( (CharTypeInfo) primitiveTypeInfo);
        return writableCharObjectInspector.create((HiveChar) object);
      }
    case VARCHAR:
      {
        final WritableHiveVarcharObjectInspector writableVarcharObjectInspector =
                new WritableHiveVarcharObjectInspector( (VarcharTypeInfo) primitiveTypeInfo);
        return writableVarcharObjectInspector.create((HiveVarchar) object);
      }
    case BINARY:
      return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector.create((byte[]) object);
    case TIMESTAMP:
      return ((WritableTimestampObjectInspector) objectInspector).create((Timestamp) object);
    case INTERVAL_YEAR_MONTH:
      return ((WritableHiveIntervalYearMonthObjectInspector) objectInspector).create((HiveIntervalYearMonth) object);
    case INTERVAL_DAY_TIME:
      return ((WritableHiveIntervalDayTimeObjectInspector) objectInspector).create((HiveIntervalDayTime) object);
    case DECIMAL:
      {
        final WritableHiveDecimalObjectInspector writableDecimalObjectInspector =
                new WritableHiveDecimalObjectInspector((DecimalTypeInfo) primitiveTypeInfo);
        return writableDecimalObjectInspector.create((HiveDecimal) object);
      }
    default:
      throw new Error("Unknown primitive category " + primitiveTypeInfo.getPrimitiveCategory());
    }
  }

  public Object randomWritable(int column) {
    return randomWritable(typeInfos[column], objectInspectorList.get(column));
  }

  public Object randomWritable(TypeInfo typeInfo, ObjectInspector objectInspector) {
    return randomWritable(typeInfo, objectInspector, allowNull);
  }

  public Object randomWritable(TypeInfo typeInfo, ObjectInspector objectInspector, boolean allowNull) {
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      {
        final Object object = randomPrimitiveObject(r, (PrimitiveTypeInfo) typeInfo);
        return getWritablePrimitiveObject((PrimitiveTypeInfo) typeInfo, objectInspector, object);
      }
    case LIST:
      {
        if (allowNull && r.nextInt(20) == 0) {
          return null;
        }
        // Always generate a list with at least 1 value?
        final int elementCount = 1 + r.nextInt(100);
        final StandardListObjectInspector listObjectInspector =
            (StandardListObjectInspector) objectInspector;
        final ObjectInspector elementObjectInspector =
            listObjectInspector.getListElementObjectInspector();
        final TypeInfo elementTypeInfo =
            TypeInfoUtils.getTypeInfoFromObjectInspector(
                elementObjectInspector);
        boolean isStringFamily = false;
        PrimitiveCategory primitiveCategory = null;
        if (elementTypeInfo.getCategory() == Category.PRIMITIVE) {
          primitiveCategory = ((PrimitiveTypeInfo) elementTypeInfo).getPrimitiveCategory();
          if (primitiveCategory == PrimitiveCategory.STRING ||
              primitiveCategory == PrimitiveCategory.BINARY ||
              primitiveCategory == PrimitiveCategory.CHAR ||
              primitiveCategory == PrimitiveCategory.VARCHAR) {
            isStringFamily = true;
          }
        }
        final Object listObj = listObjectInspector.create(elementCount);
        for (int i = 0; i < elementCount; i++) {
          final Object ele = randomWritable(elementTypeInfo, elementObjectInspector, allowNull);
          // UNDONE: For now, a 1-element list with a null element is a null list...
          if (ele == null && elementCount == 1) {
            return null;
          }
          if (isStringFamily && elementCount == 1) {
            switch (primitiveCategory) {
            case STRING:
              if (((Text) ele).getLength() == 0) {
                return null;
              }
              break;
            case BINARY:
              if (((BytesWritable) ele).getLength() == 0) {
                return null;
              }
              break;
            case CHAR:
              if (((HiveCharWritable) ele).getHiveChar().getStrippedValue().isEmpty()) {
                return null;
              }
              break;
            case VARCHAR:
              if (((HiveVarcharWritable) ele).getHiveVarchar().getValue().isEmpty()) {
                return null;
              }
              break;
            default:
              throw new RuntimeException("Unexpected primitive category " + primitiveCategory);
            }
          }
          listObjectInspector.set(listObj, i, ele);
        }
        return listObj;
      }
    case MAP:
      {
        if (allowNull && r.nextInt(20) == 0) {
          return null;
        }
        final int keyPairCount = r.nextInt(100);
        final StandardMapObjectInspector mapObjectInspector =
            (StandardMapObjectInspector) objectInspector;
        final ObjectInspector keyObjectInspector =
            mapObjectInspector.getMapKeyObjectInspector();
        final TypeInfo keyTypeInfo =
            TypeInfoUtils.getTypeInfoFromObjectInspector(
                keyObjectInspector);
        final ObjectInspector valueObjectInspector =
            mapObjectInspector.getMapValueObjectInspector();
        final TypeInfo valueTypeInfo =
            TypeInfoUtils.getTypeInfoFromObjectInspector(
                valueObjectInspector);
        final Object mapObj = mapObjectInspector.create();
        for (int i = 0; i < keyPairCount; i++) {
          final Object key = randomWritable(keyTypeInfo, keyObjectInspector);
          final Object value = randomWritable(valueTypeInfo, valueObjectInspector);
          mapObjectInspector.put(mapObj, key, value);
        }
        return mapObj;
      }
    case STRUCT:
      {
        if (allowNull && r.nextInt(20) == 0) {
          return null;
        }
        final StandardStructObjectInspector structObjectInspector =
            (StandardStructObjectInspector) objectInspector;
        final List<? extends StructField> fieldRefsList = structObjectInspector.getAllStructFieldRefs();
        final int fieldCount = fieldRefsList.size();
        final Object structObj = structObjectInspector.create();
        for (int i = 0; i < fieldCount; i++) {
          final StructField fieldRef = fieldRefsList.get(i);
          final ObjectInspector fieldObjectInspector =
               fieldRef.getFieldObjectInspector();
          final TypeInfo fieldTypeInfo =
              TypeInfoUtils.getTypeInfoFromObjectInspector(
                  fieldObjectInspector);
          final Object fieldObj = randomWritable(fieldTypeInfo, fieldObjectInspector);
          structObjectInspector.setStructFieldData(structObj, fieldRef, fieldObj);
        }
        return structObj;
      }
    case UNION:
      {
        final StandardUnionObjectInspector unionObjectInspector =
            (StandardUnionObjectInspector) objectInspector;
        final List<ObjectInspector> objectInspectorList = unionObjectInspector.getObjectInspectors();
        final int unionCount = objectInspectorList.size();
        final byte tag = (byte) r.nextInt(unionCount);
        final ObjectInspector fieldObjectInspector =
            objectInspectorList.get(tag);
        final TypeInfo fieldTypeInfo =
            TypeInfoUtils.getTypeInfoFromObjectInspector(
                fieldObjectInspector);
        final Object fieldObj = randomWritable(fieldTypeInfo, fieldObjectInspector, false);
        if (fieldObj == null) {
          throw new RuntimeException();
        }
        return new StandardUnion(tag, fieldObj);
      }
    default:
      throw new RuntimeException("Unexpected category " + typeInfo.getCategory());
    }
  }

  public Object randomPrimitiveObject(int column) {
    return randomPrimitiveObject(r, primitiveTypeInfos[column]);
  }

  public static Object randomPrimitiveObject(Random r, PrimitiveTypeInfo primitiveTypeInfo) {
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
    case BOOLEAN:
      return Boolean.valueOf(r.nextBoolean());
    case BYTE:
      return Byte.valueOf((byte) r.nextInt());
    case SHORT:
      return Short.valueOf((short) r.nextInt());
    case INT:
      return Integer.valueOf(r.nextInt());
    case LONG:
      return Long.valueOf(r.nextLong());
    case DATE:
      return RandomTypeUtil.getRandDate(r);
    case FLOAT:
      return Float.valueOf(r.nextFloat() * 10 - 5);
    case DOUBLE:
      return Double.valueOf(r.nextDouble() * 10 - 5);
    case STRING:
      return RandomTypeUtil.getRandString(r);
    case CHAR:
      return getRandHiveChar(r, (CharTypeInfo) primitiveTypeInfo);
    case VARCHAR:
      return getRandHiveVarchar(r, (VarcharTypeInfo) primitiveTypeInfo);
    case BINARY:
      return getRandBinary(r, 1 + r.nextInt(100));
    case TIMESTAMP:
      return RandomTypeUtil.getRandTimestamp(r);
    case INTERVAL_YEAR_MONTH:
      return getRandIntervalYearMonth(r);
    case INTERVAL_DAY_TIME:
      return getRandIntervalDayTime(r);
    case DECIMAL:
      {
        HiveDecimal dec = getRandHiveDecimal(r, (DecimalTypeInfo) primitiveTypeInfo);
        return dec;
      }
    default:
      throw new Error("Unknown primitive category " + primitiveTypeInfo.getCategory());
    }
  }

  public static HiveChar getRandHiveChar(Random r, CharTypeInfo charTypeInfo) {
    final int maxLength = 1 + r.nextInt(charTypeInfo.getLength());
    final String randomString = RandomTypeUtil.getRandString(r, "abcdefghijklmnopqrstuvwxyz", 100);
    final HiveChar hiveChar = new HiveChar(randomString, maxLength);
    return hiveChar;
  }

  public static HiveVarchar getRandHiveVarchar(Random r, VarcharTypeInfo varcharTypeInfo, String alphabet) {
    final int maxLength = 1 + r.nextInt(varcharTypeInfo.getLength());
    final String randomString = RandomTypeUtil.getRandString(r, alphabet, 100);
    final HiveVarchar hiveVarchar = new HiveVarchar(randomString, maxLength);
    return hiveVarchar;
  }

  public static HiveVarchar getRandHiveVarchar(Random r, VarcharTypeInfo varcharTypeInfo) {
    return getRandHiveVarchar(r, varcharTypeInfo, "abcdefghijklmnopqrstuvwxyz");
  }

  public static byte[] getRandBinary(Random r, int len){
    final byte[] bytes = new byte[len];
    for (int j = 0; j < len; j++){
      bytes[j] = Byte.valueOf((byte) r.nextInt());
    }
    return bytes;
  }

  private static final String DECIMAL_CHARS = "0123456789";

  public static HiveDecimal getRandHiveDecimal(Random r, DecimalTypeInfo decimalTypeInfo) {
    while (true) {
      final StringBuilder sb = new StringBuilder();
      final int precision = 1 + r.nextInt(18);
      final int scale = 0 + r.nextInt(precision + 1);

      final int integerDigits = precision - scale;

      if (r.nextBoolean()) {
        sb.append("-");
      }

      if (integerDigits == 0) {
        sb.append("0");
      } else {
        sb.append(RandomTypeUtil.getRandString(r, DECIMAL_CHARS, integerDigits));
      }
      if (scale != 0) {
        sb.append(".");
        sb.append(RandomTypeUtil.getRandString(r, DECIMAL_CHARS, scale));
      }
      return HiveDecimal.create(sb.toString());
    }
  }

  public static HiveIntervalYearMonth getRandIntervalYearMonth(Random r) {
    final String yearMonthSignStr = r.nextInt(2) == 0 ? "" : "-";
    final String intervalYearMonthStr = String.format("%s%d-%d",
        yearMonthSignStr,
        Integer.valueOf(1800 + r.nextInt(500)),  // year
        Integer.valueOf(0 + r.nextInt(12)));     // month
    final HiveIntervalYearMonth intervalYearMonthVal = HiveIntervalYearMonth.valueOf(intervalYearMonthStr);
    return intervalYearMonthVal;
  }

  public static HiveIntervalDayTime getRandIntervalDayTime(Random r) {
    String optionalNanos = "";
    if (r.nextInt(2) == 1) {
      optionalNanos = String.format(".%09d",
          Integer.valueOf(0 + r.nextInt(DateUtils.NANOS_PER_SEC)));
    }
    final String yearMonthSignStr = r.nextInt(2) == 0 ? "" : "-";
    final String dayTimeStr = String.format("%s%d %02d:%02d:%02d%s",
        yearMonthSignStr,
        Integer.valueOf(1 + r.nextInt(28)),      // day
        Integer.valueOf(0 + r.nextInt(24)),      // hour
        Integer.valueOf(0 + r.nextInt(60)),      // minute
        Integer.valueOf(0 + r.nextInt(60)),      // second
        optionalNanos);
    final HiveIntervalDayTime intervalDayTimeVal = HiveIntervalDayTime.valueOf(dayTimeStr);
    return intervalDayTimeVal;
  }
}
