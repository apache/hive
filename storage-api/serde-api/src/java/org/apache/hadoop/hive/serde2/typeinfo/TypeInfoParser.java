package org.apache.hadoop.hive.serde2.typeinfo;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveTypeEntry;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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
@InterfaceStability.Evolving
@InterfaceAudience.LimitedPrivate({"Standalone Metastore", "Hive"})
public class TypeInfoParser {

  /**
   * Make some of the TypeInfo parsing available as a utility.
   */
  public static PrimitiveParts parsePrimitiveParts(String typeInfoString) {
    TypeInfoParser parser = new TypeInfoParser(typeInfoString);
    return parser.parsePrimitiveParts();
  }

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
          && null == PrimitiveTypeEntry.fromTypeName(t.text)
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
        PrimitiveTypeEntry.fromTypeName(t.text);
    if (typeEntry != null && typeEntry.primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN ) {
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
          if (typeEntry.primitiveCategory == PrimitiveObjectInspector.PrimitiveCategory.VARCHAR) {
            PrimitiveTypeInfoValidationUtils.validateVarcharParameter(length);
            return TypeInfoFactory.getVarcharTypeInfo(length);
          } else {
            PrimitiveTypeInfoValidationUtils.validateCharParameter(length);
            return TypeInfoFactory.getCharTypeInfo(length);
          }
        } else if (params.length > 1) {
          throw new IllegalArgumentException(
              "Type " + typeEntry.typeName+ " only takes one parameter, but " +
              params.length + " is seen");
        }

      case DECIMAL:
        int precision = serdeConstants.USER_DEFAULT_PRECISION;
        int scale = serdeConstants.USER_DEFAULT_SCALE;
        if (params == null || params.length == 0) {
          // It's possible that old metadata still refers to "decimal" as a column type w/o
          // precision/scale. In this case, the default (10,0) is assumed. Thus, do nothing here.
        } else if (params.length == 1) {
          // only precision is specified
          precision = Integer.valueOf(params[0]);
          PrimitiveTypeInfoValidationUtils.validateParameter(precision, scale);
        } else if (params.length == 2) {
          // New metadata always have two parameters.
          precision = Integer.parseInt(params[0]);
          scale = Integer.parseInt(params[1]);
          PrimitiveTypeInfoValidationUtils.validateParameter(precision, scale);
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

  public static class PrimitiveParts {
    public String  typeName;
    public String[] typeParams;
  }
}
