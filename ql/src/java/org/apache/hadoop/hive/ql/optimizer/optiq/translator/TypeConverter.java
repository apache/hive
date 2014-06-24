package org.apache.hadoop.hive.ql.optimizer.optiq.translator;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.sql.type.SqlTypeName;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class TypeConverter {

  public static RelDataType getType(RelOptCluster cluster, RowResolver rr, List<String> neededCols) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
    RowSchema rs = rr.getRowSchema();
    List<RelDataType> fieldTypes = new LinkedList<RelDataType>();
    List<String> fieldNames = new LinkedList<String>();

    for (ColumnInfo ci : rs.getSignature()) {
      if (neededCols == null || neededCols.contains(ci.getInternalName())) {
        fieldTypes.add(convert(ci.getType(), dtFactory));
        fieldNames.add(ci.getInternalName());
      }
    }
    return dtFactory.createStructType(fieldTypes, fieldNames);
  }

  public static RelDataType convert(TypeInfo type, RelDataTypeFactory dtFactory) {
    RelDataType convertedType = null;

    switch (type.getCategory()) {
    case PRIMITIVE:
      convertedType = convert((PrimitiveTypeInfo) type, dtFactory);
      break;
    case LIST:
      convertedType = convert((ListTypeInfo) type, dtFactory);
      break;
    case MAP:
      convertedType = convert((MapTypeInfo) type, dtFactory);
      break;
    case STRUCT:
      convertedType = convert((StructTypeInfo) type, dtFactory);
      break;
    case UNION:
      convertedType = convert((UnionTypeInfo) type, dtFactory);
      break;
    }
    return convertedType;
  }

  public static RelDataType convert(PrimitiveTypeInfo type, RelDataTypeFactory dtFactory) {
    RelDataType convertedType = null;

    switch (type.getPrimitiveCategory()) {
    case VOID:
      // @todo: followup on VOID type in hive
      convertedType = dtFactory.createSqlType(SqlTypeName.OTHER);
      break;
    case BOOLEAN:
      convertedType = dtFactory.createSqlType(SqlTypeName.BOOLEAN);
      break;
    case BYTE:
      convertedType = dtFactory.createSqlType(SqlTypeName.TINYINT);
      break;
    case SHORT:
      convertedType = dtFactory.createSqlType(SqlTypeName.SMALLINT);
      break;
    case INT:
      convertedType = dtFactory.createSqlType(SqlTypeName.INTEGER);
      break;
    case LONG:
      convertedType = dtFactory.createSqlType(SqlTypeName.BIGINT);
      break;
    case FLOAT:
      convertedType = dtFactory.createSqlType(SqlTypeName.FLOAT);
      break;
    case DOUBLE:
      convertedType = dtFactory.createSqlType(SqlTypeName.DOUBLE);
      break;
    case STRING:
      convertedType = dtFactory.createSqlType(SqlTypeName.VARCHAR, 1);
      break;
    case DATE:
      convertedType = dtFactory.createSqlType(SqlTypeName.DATE);
      break;
    case TIMESTAMP:
      convertedType = dtFactory.createSqlType(SqlTypeName.TIMESTAMP);
      break;
    case BINARY:
      convertedType = dtFactory.createSqlType(SqlTypeName.BINARY);
      break;
    case DECIMAL:
      convertedType = dtFactory.createSqlType(SqlTypeName.DECIMAL);
      break;
    case VARCHAR:
      convertedType = dtFactory.createSqlType(SqlTypeName.VARCHAR,
          ((BaseCharTypeInfo) type).getLength());
      break;
    case CHAR:
      convertedType = dtFactory.createSqlType(SqlTypeName.CHAR,
          ((BaseCharTypeInfo) type).getLength());
      break;
    case UNKNOWN:
      convertedType = dtFactory.createSqlType(SqlTypeName.OTHER);
      break;
    }

    return convertedType;
  }

  public static RelDataType convert(ListTypeInfo lstType, RelDataTypeFactory dtFactory) {
    RelDataType elemType = convert(lstType.getListElementTypeInfo(), dtFactory);
    return dtFactory.createArrayType(elemType, -1);
  }

  public static RelDataType convert(MapTypeInfo mapType, RelDataTypeFactory dtFactory) {
    RelDataType keyType = convert(mapType.getMapKeyTypeInfo(), dtFactory);
    RelDataType valueType = convert(mapType.getMapValueTypeInfo(), dtFactory);
    return dtFactory.createMapType(keyType, valueType);
  }

  public static RelDataType convert(StructTypeInfo structType, final RelDataTypeFactory dtFactory) {
    List<RelDataType> fTypes = Lists.transform(structType.getAllStructFieldTypeInfos(),
        new Function<TypeInfo, RelDataType>() {
          public RelDataType apply(TypeInfo tI) {
            return convert(tI, dtFactory);
          }
        });
    return dtFactory.createStructType(fTypes, structType.getAllStructFieldNames());
  }

  public static RelDataType convert(UnionTypeInfo unionType, RelDataTypeFactory dtFactory) {
    // @todo what do we about unions?
    throw new UnsupportedOperationException();
  }

}
