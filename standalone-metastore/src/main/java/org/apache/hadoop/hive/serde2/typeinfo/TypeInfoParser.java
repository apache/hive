/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.typeinfo;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.ColumnType;

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
@LimitedPrivate("Hive")
public class TypeInfoParser {

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

  public TypeInfoParser(String typeInfoString, ITypeInfoFactory typeInfoFactory) {
    this.typeInfoString = typeInfoString;
    this.typeInfoFactory = typeInfoFactory;
    typeInfoTokens = tokenize(typeInfoString);
  }

  private final String typeInfoString;
  private final ArrayList<Token> typeInfoTokens;
  private final ITypeInfoFactory typeInfoFactory;
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
      if (!ColumnType.LIST_TYPE_NAME.equals(t.text)
          && !ColumnType.MAP_TYPE_NAME.equals(t.text)
          && !ColumnType.STRUCT_TYPE_NAME.equals(t.text)
          && !ColumnType.UNION_TYPE_NAME.equals(t.text)
          && !ColumnType.PrimitiveTypes.contains(t.text)
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
    if (ColumnType.PrimitiveTypes.contains(t.text)) {
      String[] params = parseParams();
      switch (t.text) {
      case ColumnType.CHAR_TYPE_NAME:
      case ColumnType.VARCHAR_TYPE_NAME:
        if (params == null || params.length == 0) {
          throw new IllegalArgumentException(t.text
              + " type is specified without length: " + typeInfoString);
        }

        int length = 1;
        if (params.length == 1) {
          length = Integer.parseInt(params[0]);
          if (ColumnType.VARCHAR_TYPE_NAME.equals(t.text)) {
            return typeInfoFactory.getPrimitiveTypeInfo(ColumnType.VARCHAR_TYPE_NAME, length);
          } else {
            return typeInfoFactory.getPrimitiveTypeInfo(ColumnType.CHAR_TYPE_NAME, length);
          }
        } else if (params.length > 1) {
          throw new IllegalArgumentException(
              "Type " + t.text + " only takes one parameter, but " +
              params.length + " is seen");
        }

      case ColumnType.DECIMAL_TYPE_NAME:
        //TODO do we need to support this? this works only by luck because
        //standalone-metastore depends on storage-api and HiveDecimal happens to be
        //in storage-api
        int precision = HiveDecimal.USER_DEFAULT_PRECISION;
        int scale = HiveDecimal.USER_DEFAULT_SCALE;
        if (params == null || params.length == 0) {
          // It's possible that old metadata still refers to "decimal" as a column type w/o
          // precision/scale. In this case, the default (10,0) is assumed. Thus, do nothing here.
        } else if (params.length == 1) {
          // only precision is specified
          precision = Integer.valueOf(params[0]);
        } else if (params.length == 2) {
          // New metadata always have two parameters.
          precision = Integer.parseInt(params[0]);
          scale = Integer.parseInt(params[1]);
        } else if (params.length > 2) {
          throw new IllegalArgumentException("Type decimal only takes two parameter, but " +
              params.length + " is seen");
        }
        return typeInfoFactory.getPrimitiveTypeInfo(ColumnType.DECIMAL_TYPE_NAME, precision, scale);

      default:
        return typeInfoFactory.getPrimitiveTypeInfo(t.text);
      }
    }

    // Is this a list type?
    if (ColumnType.LIST_TYPE_NAME.equals(t.text)) {
      expect("<");
      TypeInfo listElementType = parseType();
      expect(">");
      return typeInfoFactory.getListTypeInfo(listElementType);
    }

    // Is this a map type?
    if (ColumnType.MAP_TYPE_NAME.equals(t.text)) {
      expect("<");
      TypeInfo mapKeyType = parseType();
      expect(",");
      TypeInfo mapValueType = parseType();
      expect(">");
      return typeInfoFactory.getMapTypeInfo(mapKeyType, mapValueType);
    }

    // Is this a struct type?
    if (ColumnType.STRUCT_TYPE_NAME.equals(t.text)) {
      ArrayList<String> fieldNames = new ArrayList<>();
      ArrayList<TypeInfo> fieldTypeInfos = new ArrayList<>();
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

      return typeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
    }
    // Is this a union type?
    if (ColumnType.UNION_TYPE_NAME.equals(t.text)) {
      List<TypeInfo> objectTypeInfos = new ArrayList<>();
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

      return typeInfoFactory.getUnionTypeInfo(objectTypeInfos);
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
