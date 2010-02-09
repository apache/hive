package org.apache.hadoop.hive.serde2.typeinfo;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;

/**
 * TypeInfoUtils.
 *
 */
public final class TypeInfoUtils {

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
   * GenericArrayType (Map<String,String>[]). Otherwise return null.
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
   *          return a List<TypeInfo> with the specified size by repeating the
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
      return Character.isLetterOrDigit(c) || c == '_' || c == '.';
    }

    /**
     * Tokenize the typeInfoString. The rule is simple: all consecutive
     * alphadigits and '_', '.' are in one token, and all other characters are
     * one character per token.
     * 
     * tokenize("map<int,string>") should return
     * ["map","<","int",",","string",">"]
     */
    private static ArrayList<Token> tokenize(String typeInfoString) {
      ArrayList<Token> tokens = new ArrayList<Token>(0);
      int begin = 0;
      int end = 1;
      while (end <= typeInfoString.length()) {
        // last character ends a token?
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
        if (!Constants.LIST_TYPE_NAME.equals(t.text)
            && !Constants.MAP_TYPE_NAME.equals(t.text)
            && !Constants.STRUCT_TYPE_NAME.equals(t.text)
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

    private TypeInfo parseType() {

      Token t = expect("type");

      // Is this a primitive type?
      PrimitiveTypeEntry primitiveType = PrimitiveObjectInspectorUtils
          .getTypeEntryFromTypeName(t.text);
      if (primitiveType != null
          && !primitiveType.primitiveCategory.equals(PrimitiveCategory.UNKNOWN)) {
        return TypeInfoFactory.getPrimitiveTypeInfo(primitiveType.typeName);
      }

      // Is this a list type?
      if (Constants.LIST_TYPE_NAME.equals(t.text)) {
        expect("<");
        TypeInfo listElementType = parseType();
        expect(">");
        return TypeInfoFactory.getListTypeInfo(listElementType);
      }

      // Is this a map type?
      if (Constants.MAP_TYPE_NAME.equals(t.text)) {
        expect("<");
        TypeInfo mapKeyType = parseType();
        expect(",");
        TypeInfo mapValueType = parseType();
        expect(">");
        return TypeInfoFactory.getMapTypeInfo(mapKeyType, mapValueType);
      }

      // Is this a struct type?
      if (Constants.STRUCT_TYPE_NAME.equals(t.text)) {
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
          Token name = expect("name");
          fieldNames.add(name.text);
          expect(":");
          fieldTypeInfos.add(parseType());
        } while (true);

        return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
      }

      throw new RuntimeException("Internal error parsing position "
          + t.position + " of '" + typeInfoString + "'");
    }

  }

  static HashMap<TypeInfo, ObjectInspector> cachedStandardObjectInspector =
      new HashMap<TypeInfo, ObjectInspector>();

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
        result = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(((PrimitiveTypeInfo) typeInfo)
            .getPrimitiveCategory());
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
      default: {
        result = null;
      }
      }
      cachedStandardObjectInspector.put(typeInfo, result);
    }
    return result;
  }

  static HashMap<TypeInfo, ObjectInspector> cachedStandardJavaObjectInspector =
      new HashMap<TypeInfo, ObjectInspector>();

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
            .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
            .getTypeEntryFromTypeName(typeInfo.getTypeName()).primitiveCategory);
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
      default: {
        result = null;
      }
      }
      cachedStandardJavaObjectInspector.put(typeInfo, result);
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

    // Recursively going into ObjectInspector structure
    TypeInfo result = null;
    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      result = TypeInfoFactory.getPrimitiveTypeInfo(poi.getTypeName());
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
    default: {
      throw new RuntimeException("Unknown ObjectInspector category!");
    }
    }
    return result;
  }

  public static ArrayList<TypeInfo> getTypeInfosFromTypeString(String typeString) {
    TypeInfoParser parser = new TypeInfoParser(typeString);
    return parser.parseTypeInfos();
  }

  public static TypeInfo getTypeInfoFromTypeString(String typeString) {
    TypeInfoParser parser = new TypeInfoParser(typeString);
    return parser.parseTypeInfos().get(0);
  }
}
