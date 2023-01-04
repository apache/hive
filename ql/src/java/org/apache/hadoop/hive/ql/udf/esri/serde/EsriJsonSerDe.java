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
package org.apache.hadoop.hive.ql.udf.esri.serde;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.JsonGeometryException;
import com.esri.core.geometry.JsonReader;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.OperatorImportFromJson;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class EsriJsonSerDe extends BaseJsonSerDe {

  @Override
  protected String outGeom(OGCGeometry geom) {
    return geom.asJson();
  }

  @Override
  protected OGCGeometry parseGeom(JsonParser parser) {
    MapGeometry mapGeom =
        OperatorImportFromJson.local().execute(Geometry.Type.Unknown, new HiveJsonParserReader(parser));
    return OGCGeometry.createFromEsriGeometry(mapGeom.getGeometry(), mapGeom.getSpatialReference());
  }
}

class HiveJsonParserReader implements JsonReader {
  private JsonParser m_jsonParser;

  public HiveJsonParserReader(JsonParser jsonParser) {
    this.m_jsonParser = jsonParser;
  }

  public static JsonReader createFromString(String str) {
    try {
      JsonFactory factory = new JsonFactory();
      JsonParser jsonParser = factory.createParser(str);
      jsonParser.nextToken();
      return new com.esri.core.geometry.JsonParserReader(jsonParser);
    } catch (Exception var3) {
      throw new JsonGeometryException(var3.getMessage());
    }
  }

  public static JsonReader createFromStringNNT(String str) {
    try {
      JsonFactory factory = new JsonFactory();
      JsonParser jsonParser = factory.createParser(str);
      return new com.esri.core.geometry.JsonParserReader(jsonParser);
    } catch (Exception var3) {
      throw new JsonGeometryException(var3.getMessage());
    }
  }

  private static Token mapToken(JsonToken token) {
    if (token == JsonToken.END_ARRAY) {
      return Token.END_ARRAY;
    } else if (token == JsonToken.END_OBJECT) {
      return Token.END_OBJECT;
    } else if (token == JsonToken.FIELD_NAME) {
      return Token.FIELD_NAME;
    } else if (token == JsonToken.START_ARRAY) {
      return Token.START_ARRAY;
    } else if (token == JsonToken.START_OBJECT) {
      return Token.START_OBJECT;
    } else if (token == JsonToken.VALUE_FALSE) {
      return Token.VALUE_FALSE;
    } else if (token == JsonToken.VALUE_NULL) {
      return Token.VALUE_NULL;
    } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
      return Token.VALUE_NUMBER_FLOAT;
    } else if (token == JsonToken.VALUE_NUMBER_INT) {
      return Token.VALUE_NUMBER_INT;
    } else if (token == JsonToken.VALUE_STRING) {
      return Token.VALUE_STRING;
    } else if (token == JsonToken.VALUE_TRUE) {
      return Token.VALUE_TRUE;
    } else if (token == null) {
      return null;
    } else {
      throw new JsonGeometryException("unexpected token");
    }
  }

  public Token nextToken() throws JsonGeometryException {
    try {
      JsonToken token = this.m_jsonParser.nextToken();
      return mapToken(token);
    } catch (Exception var2) {
      throw new JsonGeometryException(var2);
    }
  }

  public Token currentToken() throws JsonGeometryException {
    try {
      return mapToken(this.m_jsonParser.getCurrentToken());
    } catch (Exception var2) {
      throw new JsonGeometryException(var2);
    }
  }

  public void skipChildren() throws JsonGeometryException {
    try {
      this.m_jsonParser.skipChildren();
    } catch (Exception var2) {
      throw new JsonGeometryException(var2);
    }
  }

  public String currentString() throws JsonGeometryException {
    try {
      return this.m_jsonParser.getText();
    } catch (Exception var2) {
      throw new JsonGeometryException(var2);
    }
  }

  public double currentDoubleValue() throws JsonGeometryException {
    try {
      return this.m_jsonParser.getValueAsDouble();
    } catch (Exception var2) {
      throw new JsonGeometryException(var2);
    }
  }

  public int currentIntValue() throws JsonGeometryException {
    try {
      return this.m_jsonParser.getValueAsInt();
    } catch (Exception var2) {
      throw new JsonGeometryException(var2);
    }
  }

  public boolean currentBooleanValue() {
    Token t = this.currentToken();
    if (t == Token.VALUE_TRUE) {
      return true;
    } else if (t == Token.VALUE_FALSE) {
      return false;
    } else {
      throw new JsonGeometryException("Not a boolean");
    }
  }
}

