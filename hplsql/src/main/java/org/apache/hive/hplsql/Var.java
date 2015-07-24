/**
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

package org.apache.hive.hplsql;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Variable or the result of expression 
 */
public class Var {

	// Data types
	public enum Type {BOOL, CURSOR, DATE, DECIMAL, FILE, IDENT, BIGINT, INTERVAL, RS_LOCATOR, STRING, STRINGLIST, TIMESTAMP, NULL};
	public static Var Empty = new Var();
	public static Var Null = new Var(Type.NULL);
	
	public String name;
	public Type type; 
	public Object value;
	
	int len;
	int scale;
	
	public Var() {
	  type = Type.NULL;  
	}
	
	public Var(Var var) {
	  name = var.name;
    type = var.type;
    value = var.value;
    len = var.len;
    scale = var.scale;
  }
	
	public Var(Long value) {
    this.type = Type.BIGINT;
    this.value = value;
	}
	
	public Var(BigDecimal value) {
    this.type = Type.DECIMAL;
    this.value = value;
  }
  
	public Var(String name, Long value) {
    this.type = Type.BIGINT;
    this.name = name;    
    this.value = value;
  }
  
	public Var(String value) {
    this.type = Type.STRING;
    this.value = value;
  }
  
	public Var(Date value) {
    this.type = Type.DATE;
    this.value = value;
  }

	public Var(Timestamp value, int scale) {
    this.type = Type.TIMESTAMP;
    this.value = value;
    this.scale = scale;
  }
	
	public Var(Interval value) {
    this.type = Type.INTERVAL;
    this.value = value;
  }

	public Var(ArrayList<String> value) {
    this.type = Type.STRINGLIST;
    this.value = value;
  }
  
	public Var(Boolean b) {
    type = Type.BOOL;
    value = b;
  }
	
	public Var(Type type, String name) {
    this.type = type;
    this.name = name;
  }
  
	public Var(Type type, Object value) {
    this.type = type;
    this.value = value;
  }
  
	public Var(String name, Type type, Object value) {
    this.name = name;
    this.type = type;
    this.value = value;
  }
	
	public Var(Type type) {
    this.type = type;
  }

	public Var(String name, String type, String len, String scale, Var def) {
	  this.name = name;
	  setType(type);	  
	  if (len != null) {
	    this.len = Integer.parseInt(len);
	  }
    if (scale != null) {
	    this.scale = Integer.parseInt(scale);
    }
    if (def != null) {
      cast(def);
    }
	}
	
	/**
	 * Cast a new value to the variable 
	 */
	public Var cast(Var val) {
	  if (val == null || val.value == null) {
	    value = null;
	  }
	  else if (type == val.type && type == Type.STRING) {
	    cast((String)val.value);
	  }
	  else if (type == val.type) {
	    value = val.value;
	  }
	  else if (type == Type.STRING) {
	    cast(val.toString());
	  }
	  else if (type == Type.DATE) {
	    value = Utils.toDate(val.toString());
    }
    else if (type == Type.TIMESTAMP) {
      value = Utils.toTimestamp(val.toString());
    }
	  return this;
	}

  /**
   * Cast a new string value to the variable 
   */
  public Var cast(String val) {
    if (type == Type.STRING) {
      if (len != 0 ) {
        int l = val.length();
        if (l > len) {
          value = val.substring(0, len);
          return this;
        }
      }
      value = val;
    }
    return this;
  }
	
	/**
	 * Set the new value 
	 */
	public void setValue(String str) {
	  if(type == Type.STRING) {
	    value = str;
	  }
	}
	
	public Var setValue(Long val) {
    if (type == Type.BIGINT) {
      value = val;
    }
    return this;
  }
	
	public Var setValue(Boolean val) {
    if (type == Type.BOOL) {
      value = val;
    }
    return this;
  }
	
	public void setValue(Object value) {
    this.value = value;
  }
	
	/**
   * Set the new value from a result set
   */
  public Var setValue(ResultSet rs, ResultSetMetaData rsm, int idx) throws SQLException {
    int type = rsm.getColumnType(idx);
    if (type == java.sql.Types.CHAR || type == java.sql.Types.VARCHAR) {
      cast(new Var(rs.getString(idx)));
    }
    else if (type == java.sql.Types.INTEGER || type == java.sql.Types.BIGINT) {
      cast(new Var(new Long(rs.getLong(idx))));
    }
    else if (type == java.sql.Types.DECIMAL || type == java.sql.Types.NUMERIC) {
      cast(new Var(rs.getBigDecimal(idx)));
    }
    return this;
  }
	
	/**
	 * Set the data type from string representation
	 */
	void setType(String type) {
	  this.type = defineType(type);
	}
	
	/**
   * Set the data type from JDBC type code
   */
  void setType(int type) {
    this.type = defineType(type);
  }	
	
	/**
   * Define the data type from string representation
   */
  public static Type defineType(String type) {
    if (type == null) {
      return Type.NULL;
    }    
    else if (type.equalsIgnoreCase("INT") || type.equalsIgnoreCase("INTEGER")) {
      return Type.BIGINT;
    }
    else if (type.equalsIgnoreCase("CHAR") || type.equalsIgnoreCase("VARCHAR") || type.equalsIgnoreCase("STRING")) {
      return Type.STRING;
    }
    else if (type.equalsIgnoreCase("DEC") || type.equalsIgnoreCase("DECIMAL") || type.equalsIgnoreCase("NUMERIC")) {
      return Type.DECIMAL;
    }
    else if (type.equalsIgnoreCase("DATE")) {
      return Type.DATE;
    }
    else if (type.equalsIgnoreCase("TIMESTAMP")) {
      return Type.TIMESTAMP;
    }
    else if (type.equalsIgnoreCase("SYS_REFCURSOR")) {
      return Type.CURSOR;
    }
    else if (type.equalsIgnoreCase("UTL_FILE.FILE_TYPE")) {
      return Type.FILE;
    }
    else if (type.toUpperCase().startsWith("RESULT_SET_LOCATOR")) {
      return Type.RS_LOCATOR;
    }
    return Type.NULL;
  }
  
  /**
   * Define the data type from JDBC type code
   */
  public static Type defineType(int type) {
    if (type == java.sql.Types.CHAR || type == java.sql.Types.VARCHAR) {
      return Type.STRING;
    }
    else if (type == java.sql.Types.INTEGER || type == java.sql.Types.BIGINT) {
      return Type.BIGINT;
    }
    return Type.NULL;
  }
	
	/**
	 * Remove value
	 */
	public void removeValue() {
	  type = Type.NULL;  
    name = null;
    value = null;
    len = 0;
    scale = 0;
	}
	
	/*
	 * Compare values
	 */
	@Override
  public boolean equals(Object obj) {
	  if (this == obj) {
      return true;
	  }
	  else if (obj == null || this.value == null) {
      return false;
    }
	  else if (getClass() != obj.getClass()) {
      return false;
	  }
	  
    Var var = (Var)obj;    
    if (type == Type.BIGINT && var.type == Type.BIGINT &&
       ((Long)value).longValue() == ((Long)var.value).longValue()) {
      return true;
    }
    else if (type == Type.STRING && var.type == Type.STRING &&
            ((String)value).equals((String)var.value)) {
      return true;
    }
    return false;
	}
	
	/*
   * Compare values
   */
  public int compareTo(Var v) {
    if (this == v) {
      return 0;
    }
    else if (v == null) {
      return -1;
    }
    else if (type == Type.BIGINT && v.type == Type.BIGINT) {
      return ((Long)value).compareTo((Long)v.value);
    }
    else if (type == Type.STRING && v.type == Type.STRING) {
      return ((String)value).compareTo((String)v.value);
    }
    return -1;
  }
	
	 /**
   * Increment an integer value
   */
  public Var increment(Long i) {
    if (type == Type.BIGINT) {
      value = new Long(((Long)value).longValue() + i);
    }
    return this;
  }

  /**
  * Decrement an integer value
  */
 public Var decrement(Long i) {
   if (type == Type.BIGINT) {
     value = new Long(((Long)value).longValue() - i);
   }
   return this;
 }
  
	/**
	 * Return an integer value
	 */
	public int intValue() {
	  if (type == Type.BIGINT) {
	    return ((Long)value).intValue();
	  }
	  return -1;
	}
	
	/**
	 * Return true/false for BOOL type
	 */
	public boolean isTrue() {
	  if(type == Type.BOOL && value != null) {
	    return ((Boolean)value).booleanValue();
	  }
	  return false;
	}
	
	/**
	 * Check if the variable contains NULL
	 */
	public boolean isNull() {
    if (type == Type.NULL || value == null) {
      return true;
    }
    return false;
  }
	
	/**
	 * Convert value to String
	 */
	@Override
  public String toString() {
	  if (type == Type.IDENT) {
      return name;
    }   
	  else if (value == null) {
	    return null;
	  }
	  else if (type == Type.BIGINT) {
	    return ((Long)value).toString();
	  }
	  else if (type == Type.STRING) {
      return (String)value;
    }
    else if (type == Type.DATE) {
      return ((Date)value).toString();
    }
    else if (type == Type.TIMESTAMP) {
      int len = 19;
      String t = ((Timestamp)value).toString();   // .0 returned if the fractional part not set
      if (scale > 0) {
        len += scale + 1;
      }
      if (t.length() > len) {
        t = t.substring(0, len);
      }
      return t;
    }
	  return value.toString();
	}

  /**
   * Convert value to SQL string - string literals are quoted and escaped, ab'c -> 'ab''c'
   */
  public String toSqlString() {
    if (value == null) {
      return "NULL";
    }
    else if (type == Type.STRING) {
      return Utils.quoteString((String)value);
    }
    return toString();
  }
	
  /**
   * Set variable name
   */
  public void setName(String name) {
    this.name = name;
  }
  
	/**
	 * Get variable name
	 */
	public String getName() {
	  return name;
	}
}
