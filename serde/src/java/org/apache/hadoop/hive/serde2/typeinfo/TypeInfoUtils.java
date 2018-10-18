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
package org.apache.hadoop.hive.serde2.typeinfo;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TypeInfoUtils.
 *
 */
public final class TypeInfoUtils {

  protected static final Logger LOG = LoggerFactory.getLogger(TypeInfoUtils.class);

  public static List<PrimitiveCategory> numericTypeList = new ArrayList<PrimitiveCategory>();
  // The ordering of types here is used to determine which numeric types
  // are common/convertible to one another. Probably better to rely on the
  // ordering explicitly defined here than to assume that the enum values
  // that were arbitrarily assigned in PrimitiveCategory work for our purposes.
  public static EnumMap<PrimitiveCategory, Integer> numericTypes =
      new EnumMap<PrimitiveCategory, Integer>(PrimitiveCategory.class);
  static {
    registerNumericType(PrimitiveCategory.BYTE, 1);
    registerNumericType(PrimitiveCategory.SHORT, 2);
    registerNumericType(PrimitiveCategory.INT, 3);
    registerNumericType(PrimitiveCategory.LONG, 4);
    registerNumericType(PrimitiveCategory.DECIMAL, 5);
    registerNumericType(PrimitiveCategory.FLOAT, 6);
    registerNumericType(PrimitiveCategory.DOUBLE, 7);
    registerNumericType(PrimitiveCategory.STRING, 8);
  }

  public static List<PrimitiveCategory> dateTypeList = new ArrayList<PrimitiveCategory>();
  public static EnumMap<PrimitiveCategory, Integer> dateTypes =
      new EnumMap<PrimitiveCategory, Integer>(PrimitiveCategory.class);
  static {
    registerDateType(PrimitiveCategory.DATE, 1);
    registerDateType(PrimitiveCategory.TIMESTAMP, 2);
    registerDateType(PrimitiveCategory.TIMESTAMPLOCALTZ, 3);
  }

  private TypeInfoUtils() {
    // prevent instantiation
  }

  /**
   * Return the extended TypeInfo from a Java type. By extended TypeInfo, we
   * allow unknownType for java.lang.Object.
   *
   * @param t
   *          The Java type.
   * @param m
   *          The method, only used for generating error messages.
   */
  private static TypeInfo getExtendedTypeInfoFromJavaType(Type t, Method m) {

    if (t == Object.class) {
      return TypeInfoFactory.unknownTypeInfo;
    }

    if (t instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType) t;
      // List?
      if (List.class == (Class<?>) pt.getRawType()
          || ArrayList.class == (Class<?>) pt.getRawType()) {
        return TypeInfoFactory.getListTypeInfo(getExtendedTypeInfoFromJavaType(
            pt.getActualTypeArguments()[0], m));
      }
      // Map?
      if (Map.class == (Class<?>) pt.getRawType()
          || HashMap.class == (Class<?>) pt.getRawType()) {
        return TypeInfoFactory.getMapTypeInfo(getExtendedTypeInfoFromJavaType(
            pt.getActualTypeArguments()[0], m),
            getExtendedTypeInfoFromJavaType(pt.getActualTypeArguments()[1], m));
      }
      // Otherwise convert t to RawType so we will fall into the following if
      // block.
      t = pt.getRawType();
    }

    // Must be a class.
    if (!(t instanceof Class)) {
      throw new RuntimeException("Hive does not understand type " + t
          + " from " + m);
    }
    Class<?> c = (Class<?>) t;

    // Java Primitive Type?
    if (PrimitiveObjectInspectorUtils.isPrimitiveJavaType(c)) {
      return TypeInfoUtils
          .getTypeInfoFromObjectInspector(PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveJavaType(c).primitiveCategory));
    }

    // Java Primitive Class?
    if (PrimitiveObjectInspectorUtils.isPrimitiveJavaClass(c)) {
      return TypeInfoUtils
          .getTypeInfoFromObjectInspector(PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveJavaClass(c).primitiveCategory));
    }

    // Primitive Writable class?
    if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(c)) {
      return TypeInfoUtils
          .getTypeInfoFromObjectInspector(PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveWritableClass(c).primitiveCategory));
    }

    // Must be a struct
    Field[] fields = ObjectInspectorUtils.getDeclaredNonStaticFields(c);
    ArrayList<String> fieldNames = new ArrayList<String>(fields.length);
    ArrayList<TypeInfo> fieldTypeInfos = new ArrayList<TypeInfo>(fields.length);
    for (Field field : fields) {
      fieldNames.add(field.getName());
      fieldTypeInfos.add(getExtendedTypeInfoFromJavaType(
          field.getGenericType(), m));
    }
    return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
  }

  /**
   * Returns the array element type, if the Type is an array (Object[]), or
   * GenericArrayType (Map&lt;String,String&gt;[]). Otherwise return null.
   */
  public static Type getArrayElementType(Type t) {
    if (t instanceof Class && ((Class<?>) t).isArray()) {
      Class<?> arrayClass = (Class<?>) t;
      return arrayClass.getComponentType();
    } else if (t instanceof GenericArrayType) {
      GenericArrayType arrayType = (GenericArrayType) t;
      return arrayType.getGenericComponentType();
    }
    return null;
  }

  /**
   * Get the parameter TypeInfo for a method.
   *
   * @param size
   *          In case the last parameter of Method is an array, we will try to
   *          return a List&lt;TypeInfo&gt; with the specified size by repeating the
   *          element of the array at the end. In case the size is smaller than
   *          the minimum possible number of arguments for the method, null will
   *          be returned.
   */
  public static List<TypeInfo> getParameterTypeInfos(Method m, int size) {
    Type[] methodParameterTypes = m.getGenericParameterTypes();

    // Whether the method takes variable-length arguments
    // Whether the method takes an array like Object[],
    // or String[] etc in the last argument.
    Type lastParaElementType = TypeInfoUtils
        .getArrayElementType(methodParameterTypes.length == 0 ? null
        : methodParameterTypes[methodParameterTypes.length - 1]);
    boolean isVariableLengthArgument = (lastParaElementType != null);

    List<TypeInfo> typeInfos = null;
    if (!isVariableLengthArgument) {
      // Normal case, no variable-length arguments
      if (size != methodParameterTypes.length) {
        return null;
      }
      typeInfos = new ArrayList<TypeInfo>(methodParameterTypes.length);
      for (Type methodParameterType : methodParameterTypes) {
        typeInfos.add(getExtendedTypeInfoFromJavaType(methodParameterType, m));
      }
    } else {
      // Variable-length arguments
      if (size < methodParameterTypes.length - 1) {
        return null;
      }
      typeInfos = new ArrayList<TypeInfo>(size);
      for (int i = 0; i < methodParameterTypes.length - 1; i++) {
        typeInfos.add(getExtendedTypeInfoFromJavaType(methodParameterTypes[i],
            m));
      }
      for (int i = methodParameterTypes.length - 1; i < size; i++) {
        typeInfos.add(getExtendedTypeInfoFromJavaType(lastParaElementType, m));
      }
    }
    return typeInfos;
  }

  public static boolean hasParameters(String typeName) {
    int idx = typeName.indexOf('(');
    if (idx == -1) {
      return false;
    } else {
      return true;
    }
  }

  public static String getBaseName(String typeName) {
    int idx = typeName.indexOf('(');
    if (idx == -1) {
      return typeName;
    } else {
      return typeName.substring(0, idx);
    }
  }

  /**
   * returns true if both TypeInfos are of primitive type, and the primitive category matches.
   * @param ti1
   * @param ti2
   * @return
   */
  public static boolean doPrimitiveCategoriesMatch(TypeInfo ti1, TypeInfo ti2) {
    if (ti1.getCategory() == Category.PRIMITIVE && ti2.getCategory() == Category.PRIMITIVE) {
      if (((PrimitiveTypeInfo)ti1).getPrimitiveCategory()
          == ((PrimitiveTypeInfo)ti2).getPrimitiveCategory()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parse a recursive TypeInfo list String. For example, the following inputs
   * are valid inputs:
   * "int,string,map<string,int>,list<map<int,list<string>>>,list<struct<a:int,b:string>>"
   * The separators between TypeInfos can be ",", ":", or ";".
   *
   * In order to use this class: TypeInfoParser parser = new
   * TypeInfoParser("int,string"); ArrayList<TypeInfo> typeInfos =
   * parser.parseTypeInfos();
   */
  private static class TypeInfoParser {

    private static class Token {
      public int position;
      public String text;
      public boolean isType;

      @Override
      public String toString() {
        return "" + position + ":" + text;
      }
    };

    private static boolean isTypeChar(char c) {
      return Character.isLetterOrDigit(c) || c == '_' || c == '.' || c == ' ' || c == '$';
    }

    /**
     * Tokenize the typeInfoString. The rule is simple: all consecutive
     * alphadigits and '_', '.' are in one token, and all other characters are
     * one character per token.
     *
     * tokenize("map<int,string>") should return
     * ["map","<","int",",","string",">"]
     *
     * Note that we add '$' in new Calcite return path. As '$' will not appear
     * in any type in Hive, it is safe to do so.
     */
    private static ArrayList<Token> tokenize(String typeInfoString) {
      ArrayList<Token> tokens = new ArrayList<Token>(0);
      int begin = 0;
      int end = 1;
      while (end <= typeInfoString.length()) {
        // last character ends a token?
        // if there are quotes, all the text between the quotes
        // is considered a single token (this can happen for
        // timestamp with local time-zone)
        if (begin > 0 &&
            typeInfoString.charAt(begin - 1) == '(' &&
            typeInfoString.charAt(begin) == '\'') {
          // Ignore starting quote
          begin++;
          do {
            end++;
          } while (typeInfoString.charAt(end) != '\'');
        } else if (typeInfoString.charAt(begin) == '\'' &&
            typeInfoString.charAt(begin + 1) == ')') {
          // Ignore closing quote
          begin++;
          end++;
        }
        if (end == typeInfoString.length()
            || !isTypeChar(typeInfoString.charAt(end - 1))
            || !isTypeChar(typeInfoString.charAt(end))) {
          Token t = new Token();
          t.position = begin;
          t.text = typeInfoString.substring(begin, end);
          t.isType = isTypeChar(typeInfoString.charAt(begin));
          tokens.add(t);
          begin = end;
        }
        end++;
      }
      return tokens;
    }

    public TypeInfoParser(String typeInfoString) {
      this.typeInfoString = typeInfoString;
      typeInfoTokens = tokenize(typeInfoString);
    }

    private final String typeInfoString;
    private final ArrayList<Token> typeInfoTokens;
    private ArrayList<TypeInfo> typeInfos;
    private int iToken;

    public ArrayList<TypeInfo> parseTypeInfos() {
      typeInfos = new ArrayList<TypeInfo>();
      iToken = 0;
      while (iToken < typeInfoTokens.size()) {
        typeInfos.add(parseType());
        if (iToken < typeInfoTokens.size()) {
          Token separator = typeInfoTokens.get(iToken);
          if (",".equals(separator.text) || ";".equals(separator.text)
              || ":".equals(separator.text)) {
            iToken++;
          } else {
            throw new IllegalArgumentException(
                "Error: ',', ':', or ';' expected at position "
                + separator.position + " from '" + typeInfoString + "' "
                + typeInfoTokens);
          }
        }
      }
      return typeInfos;
    }

    private Token peek() {
      if (iToken < typeInfoTokens.size()) {
        return typeInfoTokens.get(iToken);
      } else {
        return null;
      }
    }

    private Token expect(String item) {
      return expect(item, null);
    }

    private Token expect(String item, String alternative) {
      if (iToken >= typeInfoTokens.size()) {
        throw new IllegalArgumentException("Error: " + item
            + " expected at the end of '" + typeInfoString + "'");
      }
      Token t = typeInfoTokens.get(iToken);
      if (item.equals("type")) {
        if (!serdeConstants.LIST_TYPE_NAME.equals(t.text)
            && !serdeConstants.MAP_TYPE_NAME.equals(t.text)
            && !serdeConstants.STRUCT_TYPE_NAME.equals(t.text)
            && !serdeConstants.UNION_TYPE_NAME.equals(t.text)
            && null == PrimitiveObjectInspectorUtils
            .getTypeEntryFromTypeName(t.text)
            && !t.text.equals(alternative)) {
          throw new IllegalArgumentException("Error: " + item
              + " expected at the position " + t.position + " of '"
              + typeInfoString + "' but '" + t.text + "' is found.");
        }
      } else if (item.equals("name")) {
        if (!t.isType && !t.text.equals(alternative)) {
          throw new IllegalArgumentException("Error: " + item
              + " expected at the position " + t.position + " of '"
              + typeInfoString + "' but '" + t.text + "' is found.");
        }
      } else {
        if (!item.equals(t.text) && !t.text.equals(alternative)) {
          throw new IllegalArgumentException("Error: " + item
              + " expected at the position " + t.position + " of '"
              + typeInfoString + "' but '" + t.text + "' is found.");
        }
      }
      iToken++;
      return t;
    }

    private String[] parseParams() {
      List<String> params = new LinkedList<String>();

      Token t = peek();
      if (t != null && t.text.equals("(")) {
        expect("(");

        // checking for null in the for-loop condition prevents null-ptr exception
        // and allows us to fail more gracefully with a parsing error.
        for(t = peek(); (t == null) || !t.text.equals(")"); t = expect(",",")")) {
          params.add(expect("name").text);
        }
        if (params.size() == 0) {
          throw new IllegalArgumentException(
              "type parameters expected for type string " + typeInfoString);
        }
      }

      return params.toArray(new String[params.size()]);
    }

    private TypeInfo parseType() {

      Token t = expect("type");

      // Is this a primitive type?
      PrimitiveTypeEntry typeEntry =
          PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(t.text);
      if (typeEntry != null && typeEntry.primitiveCategory != PrimitiveCategory.UNKNOWN ) {
        String[] params = parseParams();
        switch (typeEntry.primitiveCategory) {
        case CHAR:
        case VARCHAR:
          if (params == null || params.length == 0) {
            throw new IllegalArgumentException(typeEntry.typeName
                + " type is specified without length: " + typeInfoString);
          }

          int length = 1;
          if (params.length == 1) {
            length = Integer.parseInt(params[0]);
            if (typeEntry.primitiveCategory == PrimitiveCategory.VARCHAR) {
              BaseCharUtils.validateVarcharParameter(length);
              return TypeInfoFactory.getVarcharTypeInfo(length);
            } else {
              BaseCharUtils.validateCharParameter(length);
              return TypeInfoFactory.getCharTypeInfo(length);
            }
          } else if (params.length > 1) {
            throw new IllegalArgumentException(
                "Type " + typeEntry.typeName+ " only takes one parameter, but " +
                params.length + " is seen");
          }

        case DECIMAL:
          int precision = HiveDecimal.USER_DEFAULT_PRECISION;
          int scale = HiveDecimal.USER_DEFAULT_SCALE;
          if (params == null || params.length == 0) {
            // It's possible that old metadata still refers to "decimal" as a column type w/o
            // precision/scale. In this case, the default (10,0) is assumed. Thus, do nothing here.
          } else if (params.length == 1) {
            // only precision is specified
            precision = Integer.valueOf(params[0]);
            HiveDecimalUtils.validateParameter(precision, scale);
          } else if (params.length == 2) {
            // New metadata always have two parameters.
            precision = Integer.parseInt(params[0]);
            scale = Integer.parseInt(params[1]);
            HiveDecimalUtils.validateParameter(precision, scale);
          } else if (params.length > 2) {
            throw new IllegalArgumentException("Type decimal only takes two parameter, but " +
                params.length + " is seen");
          }
          return TypeInfoFactory.getDecimalTypeInfo(precision, scale);

        default:
          return TypeInfoFactory.getPrimitiveTypeInfo(typeEntry.typeName);
        }
      }

      // Is this a list type?
      if (serdeConstants.LIST_TYPE_NAME.equals(t.text)) {
        expect("<");
        TypeInfo listElementType = parseType();
        expect(">");
        return TypeInfoFactory.getListTypeInfo(listElementType);
      }

      // Is this a map type?
      if (serdeConstants.MAP_TYPE_NAME.equals(t.text)) {
        expect("<");
        TypeInfo mapKeyType = parseType();
        expect(",");
        TypeInfo mapValueType = parseType();
        expect(">");
        return TypeInfoFactory.getMapTypeInfo(mapKeyType, mapValueType);
      }

      // Is this a struct type?
      if (serdeConstants.STRUCT_TYPE_NAME.equals(t.text)) {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<TypeInfo> fieldTypeInfos = new ArrayList<TypeInfo>();
        boolean first = true;
        do {
          if (first) {
            expect("<");
            first = false;
          } else {
            Token separator = expect(">", ",");
            if (separator.text.equals(">")) {
              // end of struct
              break;
            }
          }
          Token name = expect("name",">");
          if (name.text.equals(">")) {
            break;
          }
          fieldNames.add(name.text);
          expect(":");
          fieldTypeInfos.add(parseType());
        } while (true);

        return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
      }
      // Is this a union type?
      if (serdeConstants.UNION_TYPE_NAME.equals(t.text)) {
        List<TypeInfo> objectTypeInfos = new ArrayList<TypeInfo>();
        boolean first = true;
        do {
          if (first) {
            expect("<");
            first = false;
          } else {
            Token separator = expect(">", ",");
            if (separator.text.equals(">")) {
              // end of union
              break;
            }
          }
          objectTypeInfos.add(parseType());
        } while (true);

        return TypeInfoFactory.getUnionTypeInfo(objectTypeInfos);
      }

      throw new RuntimeException("Internal error parsing position "
          + t.position + " of '" + typeInfoString + "'");
    }

    public PrimitiveParts parsePrimitiveParts() {
      PrimitiveParts parts = new PrimitiveParts();
      Token t = expect("type");
      parts.typeName = t.text;
      parts.typeParams = parseParams();
      return parts;
    }
  }

  public static class PrimitiveParts {
    public String  typeName;
    public String[] typeParams;
  }

  /**
   * Make some of the TypeInfo parsing available as a utility.
   */
  public static PrimitiveParts parsePrimitiveParts(String typeInfoString) {
    TypeInfoParser parser = new TypeInfoParser(typeInfoString);
    return parser.parsePrimitiveParts();
  }

  static ConcurrentHashMap<TypeInfo, ObjectInspector> cachedStandardObjectInspector =
      new ConcurrentHashMap<TypeInfo, ObjectInspector>();

  /**
   * Returns the standard object inspector that can be used to translate an
   * object of that typeInfo to a standard object type.
   */
  public static ObjectInspector getStandardWritableObjectInspectorFromTypeInfo(
      TypeInfo typeInfo) {
    ObjectInspector result = cachedStandardObjectInspector.get(typeInfo);
    if (result == null) {
      switch (typeInfo.getCategory()) {
      case PRIMITIVE: {
        result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            (PrimitiveTypeInfo) typeInfo);
        break;
      }
      case LIST: {
        ObjectInspector elementObjectInspector =
            getStandardWritableObjectInspectorFromTypeInfo(((ListTypeInfo) typeInfo)
            .getListElementTypeInfo());
        result = ObjectInspectorFactory
            .getStandardListObjectInspector(elementObjectInspector);
        break;
      }
      case MAP: {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        ObjectInspector keyObjectInspector =
            getStandardWritableObjectInspectorFromTypeInfo(mapTypeInfo.getMapKeyTypeInfo());
        ObjectInspector valueObjectInspector =
            getStandardWritableObjectInspectorFromTypeInfo(mapTypeInfo.getMapValueTypeInfo());
        result = ObjectInspectorFactory.getStandardMapObjectInspector(
            keyObjectInspector, valueObjectInspector);
        break;
      }
      case STRUCT: {
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        List<TypeInfo> fieldTypeInfos = structTypeInfo
            .getAllStructFieldTypeInfos();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
            fieldTypeInfos.size());
        for (int i = 0; i < fieldTypeInfos.size(); i++) {
          fieldObjectInspectors
              .add(getStandardWritableObjectInspectorFromTypeInfo(fieldTypeInfos
              .get(i)));
        }
        result = ObjectInspectorFactory.getStandardStructObjectInspector(
            fieldNames, fieldObjectInspectors);
        break;
      }
      case UNION: {
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        List<TypeInfo> objectTypeInfos = unionTypeInfo
            .getAllUnionObjectTypeInfos();
        List<ObjectInspector> fieldObjectInspectors =
          new ArrayList<ObjectInspector>(objectTypeInfos.size());
        for (int i = 0; i < objectTypeInfos.size(); i++) {
          fieldObjectInspectors
              .add(getStandardWritableObjectInspectorFromTypeInfo(objectTypeInfos
              .get(i)));
        }
        result = ObjectInspectorFactory.getStandardUnionObjectInspector(
            fieldObjectInspectors);
        break;
      }

      default: {
        result = null;
      }
      }
      ObjectInspector prev =
        cachedStandardObjectInspector.putIfAbsent(typeInfo, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<TypeInfo, ObjectInspector> cachedStandardJavaObjectInspector =
      new ConcurrentHashMap<TypeInfo, ObjectInspector>();

  /**
   * Returns the standard object inspector that can be used to translate an
   * object of that typeInfo to a standard object type.
   */
  public static ObjectInspector getStandardJavaObjectInspectorFromTypeInfo(
      TypeInfo typeInfo) {
    ObjectInspector result = cachedStandardJavaObjectInspector.get(typeInfo);
    if (result == null) {
      switch (typeInfo.getCategory()) {
      case PRIMITIVE: {
        // NOTE: we use JavaPrimitiveObjectInspector instead of
        // StandardPrimitiveObjectInspector
        result = PrimitiveObjectInspectorFactory
            .getPrimitiveJavaObjectInspector((PrimitiveTypeInfo) typeInfo);
        break;
      }
      case LIST: {
        ObjectInspector elementObjectInspector =
            getStandardJavaObjectInspectorFromTypeInfo(((ListTypeInfo) typeInfo)
            .getListElementTypeInfo());
        result = ObjectInspectorFactory
            .getStandardListObjectInspector(elementObjectInspector);
        break;
      }
      case MAP: {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        ObjectInspector keyObjectInspector = getStandardJavaObjectInspectorFromTypeInfo(mapTypeInfo
            .getMapKeyTypeInfo());
        ObjectInspector valueObjectInspector =
            getStandardJavaObjectInspectorFromTypeInfo(mapTypeInfo.getMapValueTypeInfo());
        result = ObjectInspectorFactory.getStandardMapObjectInspector(
            keyObjectInspector, valueObjectInspector);
        break;
      }
      case STRUCT: {
        StructTypeInfo strucTypeInfo = (StructTypeInfo) typeInfo;
        List<String> fieldNames = strucTypeInfo.getAllStructFieldNames();
        List<TypeInfo> fieldTypeInfos = strucTypeInfo
            .getAllStructFieldTypeInfos();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
            fieldTypeInfos.size());
        for (int i = 0; i < fieldTypeInfos.size(); i++) {
          fieldObjectInspectors
              .add(getStandardJavaObjectInspectorFromTypeInfo(fieldTypeInfos
              .get(i)));
        }
        result = ObjectInspectorFactory.getStandardStructObjectInspector(
            fieldNames, fieldObjectInspectors);
        break;
      }
      case UNION: {
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        List<TypeInfo> objectTypeInfos = unionTypeInfo
            .getAllUnionObjectTypeInfos();
        List<ObjectInspector> fieldObjectInspectors =
          new ArrayList<ObjectInspector>(objectTypeInfos.size());
        for (int i = 0; i < objectTypeInfos.size(); i++) {
          fieldObjectInspectors
              .add(getStandardJavaObjectInspectorFromTypeInfo(objectTypeInfos
              .get(i)));
        }
        result = ObjectInspectorFactory.getStandardUnionObjectInspector(
            fieldObjectInspectors);
        break;
      }
     default: {
        result = null;
      }
      }
      ObjectInspector prev =
        cachedStandardJavaObjectInspector.putIfAbsent(typeInfo, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  /**
   * Get the TypeInfo object from the ObjectInspector object by recursively
   * going into the ObjectInspector structure.
   */
  public static TypeInfo getTypeInfoFromObjectInspector(ObjectInspector oi) {
    // OPTIMIZATION for later.
    // if (oi instanceof TypeInfoBasedObjectInspector) {
    // TypeInfoBasedObjectInspector typeInfoBasedObjectInspector =
    // (ObjectInspector)oi;
    // return typeInfoBasedObjectInspector.getTypeInfo();
    // }
    if (oi == null) {
      return null;
    }

    // Recursively going into ObjectInspector structure
    TypeInfo result = null;
    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      result = poi.getTypeInfo();
      break;
    }
    case LIST: {
      ListObjectInspector loi = (ListObjectInspector) oi;
      result = TypeInfoFactory
          .getListTypeInfo(getTypeInfoFromObjectInspector(loi
          .getListElementObjectInspector()));
      break;
    }
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) oi;
      result = TypeInfoFactory.getMapTypeInfo(
          getTypeInfoFromObjectInspector(moi.getMapKeyObjectInspector()),
          getTypeInfoFromObjectInspector(moi.getMapValueObjectInspector()));
      break;
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      List<String> fieldNames = new ArrayList<String>(fields.size());
      List<TypeInfo> fieldTypeInfos = new ArrayList<TypeInfo>(fields.size());
      for (StructField f : fields) {
        fieldNames.add(f.getFieldName());
        fieldTypeInfos.add(getTypeInfoFromObjectInspector(f
            .getFieldObjectInspector()));
      }
      result = TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
      break;
    }
    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      List<TypeInfo> objectTypeInfos = new ArrayList<TypeInfo>();
      for (ObjectInspector eoi : uoi.getObjectInspectors()) {
        objectTypeInfos.add(getTypeInfoFromObjectInspector(eoi));
      }
      result = TypeInfoFactory.getUnionTypeInfo(objectTypeInfos);
      break;
    }
    default: {
      throw new RuntimeException("Unknown ObjectInspector category!");
    }
    }
    return result;
  }

  public static ArrayList<TypeInfo> typeInfosFromStructObjectInspector(
      StructObjectInspector structObjectInspector) {

    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    ArrayList<TypeInfo> typeInfoList = new ArrayList<TypeInfo>(fields.size());

    for(StructField field : fields) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
          field.getFieldObjectInspector().getTypeName());
      typeInfoList.add(typeInfo);
    }
    return typeInfoList;
  }

  public static ArrayList<TypeInfo> typeInfosFromTypeNames(List<String> typeNames) {

    ArrayList<TypeInfo> result = new ArrayList<TypeInfo>(typeNames.size());

    for(int i = 0; i < typeNames.size(); i++) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeNames.get(i));
      result.add(typeInfo);
    }
    return result;
  }

  public static ArrayList<TypeInfo> getTypeInfosFromTypeString(String typeString) {
    TypeInfoParser parser = new TypeInfoParser(typeString);
    return parser.parseTypeInfos();
  }

  public static List<String> getTypeStringsFromTypeInfo(List<TypeInfo> typeInfos) {
    if (typeInfos == null) {
      return null;
    }

    List<String> result = new ArrayList<>(typeInfos.size());
    for (TypeInfo typeInfo : typeInfos) {
      result.add(typeInfo.toString());
    }
    return result;
  }

  public static TypeInfo getTypeInfoFromTypeString(String typeString) {
    TypeInfoParser parser = new TypeInfoParser(typeString);
    return parser.parseTypeInfos().get(0);
  }

  /**
   * Given two types, determine whether conversion needs to occur to compare the two types.
   * This is needed for cases like varchar, where the TypeInfo for varchar(10) != varchar(5),
   * but there would be no need to have to convert to compare these values.
   * @param typeA
   * @param typeB
   * @return
   */
  public static boolean isConversionRequiredForComparison(TypeInfo typeA, TypeInfo typeB) {
    if (typeA.equals(typeB)) {
      return false;
    }

    if (TypeInfoUtils.doPrimitiveCategoriesMatch(typeA, typeB)) {
      return false;
    }

    return true;
  }

  /**
   * Return the character length of the type
   * @param typeInfo
   * @return
   */
  public static int getCharacterLengthForType(PrimitiveTypeInfo typeInfo) {
    switch (typeInfo.getPrimitiveCategory()) {
      case STRING:
        return HiveVarchar.MAX_VARCHAR_LENGTH;
      case CHAR:
      case VARCHAR:
        BaseCharTypeInfo baseCharTypeInfo = (BaseCharTypeInfo) typeInfo;
        return baseCharTypeInfo.getLength();
      default:
        return 0;
    }
  }

  public static synchronized void registerNumericType(PrimitiveCategory primitiveCategory, int level) {
    numericTypeList.add(primitiveCategory);
    numericTypes.put(primitiveCategory, level);
  }

  public static synchronized void registerDateType(PrimitiveCategory primitiveCategory, int level) {
    dateTypeList.add(primitiveCategory);
    dateTypes.put(primitiveCategory, level);
  }

  /**
   * Test if it's implicitly convertible for data comparison.
   */
  public static boolean implicitConvertible(PrimitiveCategory from, PrimitiveCategory to) {
    if (from == to) {
      return true;
    }

    PrimitiveGrouping fromPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(from);
    PrimitiveGrouping toPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(to);

    // Allow implicit String to Double conversion
    if (fromPg == PrimitiveGrouping.STRING_GROUP && to == PrimitiveCategory.DOUBLE) {
      return true;
    }

    // Void can be converted to any type
    if (from == PrimitiveCategory.VOID) {
      return true;
    }

    // Allow implicit String to Date conversion
    if (fromPg == PrimitiveGrouping.DATE_GROUP && toPg == PrimitiveGrouping.STRING_GROUP) {
      return true;
    }
    // Allow implicit Numeric to String conversion
    if (fromPg == PrimitiveGrouping.NUMERIC_GROUP && toPg == PrimitiveGrouping.STRING_GROUP) {
      return true;
    }
    // Allow implicit String to varchar conversion, and vice versa
    if (fromPg == PrimitiveGrouping.STRING_GROUP && toPg == PrimitiveGrouping.STRING_GROUP) {
      return true;
    }

    // Allow implicit conversion from Byte -> Integer -> Long -> Float -> Double
    // Decimal -> String
    Integer f = numericTypes.get(from);
    Integer t = numericTypes.get(to);
    if (f == null || t == null) {
      return false;
    }
    if (f.intValue() > t.intValue()) {
      return false;
    }
    return true;
  }

  /**
   * Returns whether it is possible to implicitly convert an object of Class
   * from to Class to.
   */
  public static boolean implicitConvertible(TypeInfo from, TypeInfo to) {
    if (from.equals(to)) {
      return true;
    }

    // Reimplemented to use PrimitiveCategory rather than TypeInfo, because
    // 2 TypeInfos from the same qualified type (varchar, decimal) should still be
    // seen as equivalent.
    if (from.getCategory() == Category.PRIMITIVE && to.getCategory() == Category.PRIMITIVE) {
      return implicitConvertible(
          ((PrimitiveTypeInfo) from).getPrimitiveCategory(),
          ((PrimitiveTypeInfo) to).getPrimitiveCategory());
    }
    return false;
  }
}
