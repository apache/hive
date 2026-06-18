package org.apache.hadoop.hive.metastore.metastore.impl;

import javax.jdo.Query;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.QueryWrapper;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.metastore.RawStoreAware;
import org.apache.hadoop.hive.metastore.metastore.iface.ConstraintStore;
import org.apache.hadoop.hive.metastore.metastore.iface.TableStore;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MConstraint;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MTable;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class ConstraintStoreImpl extends RawStoreAware implements ConstraintStore {

  private Configuration conf;

  @Override
  public SQLAllTableConstraints createTableWithConstraints(Table tbl, SQLAllTableConstraints constraints)
      throws InvalidObjectException, MetaException {
    baseStore.unwrap(TableStore.class).createTable(tbl);
    // Add constraints.
    // We need not do a deep retrieval of the Table Column Descriptor while persisting the
    // constraints since this transaction involving create table is not yet committed.
    if (CollectionUtils.isNotEmpty(constraints.getForeignKeys())) {
      constraints.setForeignKeys(addForeignKeys(constraints.getForeignKeys(), false, constraints.getPrimaryKeys(),
          constraints.getUniqueConstraints()));
    }
    if (CollectionUtils.isNotEmpty(constraints.getPrimaryKeys())) {
      constraints.setPrimaryKeys(addPrimaryKeys(constraints.getPrimaryKeys(), false));
    }
    if (CollectionUtils.isNotEmpty(constraints.getUniqueConstraints())) {
      constraints.setUniqueConstraints(addUniqueConstraints(constraints.getUniqueConstraints(), false));
    }
    if (CollectionUtils.isNotEmpty(constraints.getNotNullConstraints())) {
      constraints.setNotNullConstraints(addNotNullConstraints(constraints.getNotNullConstraints(), false));
    }
    if (CollectionUtils.isNotEmpty(constraints.getDefaultConstraints())) {
      constraints.setDefaultConstraints(addDefaultConstraints(constraints.getDefaultConstraints(), false));
    }
    if (CollectionUtils.isNotEmpty(constraints.getCheckConstraints())) {
      constraints.setCheckConstraints(addCheckConstraints(constraints.getCheckConstraints(), false));
    }
    return constraints;
  }

  private List<MConstraint> listAllTableConstraintsWithOptionalConstraintName(
      String catName, String dbName, String tableName, String constraintname) {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    constraintname = constraintname!=null?normalizeIdentifier(constraintname):null;
    List<MConstraint> mConstraints = null;
    List<String> constraintNames = new ArrayList<>();

    try (QueryWrapper queryForConstraintName =
             new QueryWrapper(pm.newQuery("select constraintName from org.apache.hadoop.hive.metastore.model.MConstraint  where "
        + "((parentTable.tableName == ptblname && parentTable.database.name == pdbname && " +
        "parentTable.database.catalogName == pcatname) || "
        + "(childTable != null && childTable.tableName == ctblname &&" +
        "childTable.database.name == cdbname && childTable.database.catalogName == ccatname)) " +
        (constraintname != null ? " && constraintName == constraintname" : "")));
         QueryWrapper queryForMConstraint = new QueryWrapper(pm.newQuery(MConstraint.class))) {

      queryForConstraintName.declareParameters("java.lang.String ptblname, java.lang.String pdbname,"
          + "java.lang.String pcatname, java.lang.String ctblname, java.lang.String cdbname," +
          "java.lang.String ccatname" +
          (constraintname != null ? ", java.lang.String constraintname" : ""));
      Collection<?> constraintNamesColl =
          constraintname != null ?
              ((Collection<?>) queryForConstraintName.
                  executeWithArray(tableName, dbName, catName, tableName, dbName, catName, constraintname)):
              ((Collection<?>) queryForConstraintName.
                  executeWithArray(tableName, dbName, catName, tableName, dbName, catName));
      for (Iterator<?> i = constraintNamesColl.iterator(); i.hasNext();) {
        String currName = (String) i.next();
        constraintNames.add(currName);
      }

      queryForMConstraint.setFilter("param.contains(constraintName)");
      queryForMConstraint.declareParameters("java.util.Collection param");
      Collection<?> constraints = (Collection<?>)queryForMConstraint.execute(constraintNames);
      mConstraints = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        mConstraints.add(currConstraint);
      }
    }
    return mConstraints;
  }

  private boolean constraintNameAlreadyExists(MTable table, String constraintName) {
    Query<MConstraint> constraintExistsQuery = null;
    constraintName = normalizeIdentifier(constraintName);
    constraintExistsQuery = pm.newQuery(MConstraint.class,
        "parentTable == parentTableP && constraintName == constraintNameP");
    constraintExistsQuery.declareParameters("MTable parentTableP, java.lang.String constraintNameP");
    constraintExistsQuery.setUnique(true);
    constraintExistsQuery.setResult("constraintName");
    String constraintNameIfExists = (String) constraintExistsQuery.executeWithArray(table, constraintName);
    return constraintNameIfExists != null && !constraintNameIfExists.isEmpty();
  }

  private String generateConstraintName(MTable table, String... parameters) throws MetaException {
    int hashcode = ArrayUtils.toString(parameters).hashCode() & 0xfffffff;
    int counter = 0;
    final int MAX_RETRIES = 10;
    while (counter < MAX_RETRIES) {
      String currName = (parameters.length == 0 ? "constraint_" : parameters[parameters.length-1]) +
          "_" + hashcode + "_" + System.currentTimeMillis() + "_" + (counter++);
      if (!constraintNameAlreadyExists(table, currName)) {
        return currName;
      }
    }
    throw new MetaException("Error while trying to generate the constraint name for " + ArrayUtils.toString(parameters));
  }

  @Override
  public List<SQLForeignKey> addForeignKeys(
      List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
    return addForeignKeys(fks, true, null, null);
  }

  //TODO: clean up this method
  private List<SQLForeignKey> addForeignKeys(List<SQLForeignKey> foreignKeys, boolean retrieveCD,
      List<SQLPrimaryKey> primaryKeys, List<SQLUniqueConstraint> uniqueConstraints)
      throws InvalidObjectException, MetaException {
    if (CollectionUtils.isNotEmpty(foreignKeys)) {
      List<MConstraint> mpkfks = new ArrayList<>();
      String currentConstraintName = null;
      String catName = null;
      // We start iterating through the foreign keys. This list might contain more than a single
      // foreign key, and each foreign key might contain multiple columns. The outer loop retrieves
      // the information that is common for a single key (table information) while the inner loop
      // checks / adds information about each column.
      for (int i = 0; i < foreignKeys.size(); i++) {
        if (catName == null) {
          catName = normalizeIdentifier(foreignKeys.get(i).isSetCatName() ? foreignKeys.get(i).getCatName() :
              getDefaultCatalog(conf));
        } else {
          String tmpCatName = normalizeIdentifier(foreignKeys.get(i).isSetCatName() ?
              foreignKeys.get(i).getCatName() : getDefaultCatalog(conf));
          if (!catName.equals(tmpCatName)) {
            throw new InvalidObjectException("Foreign keys cannot span catalogs");
          }
        }
        final String fkTableDB = normalizeIdentifier(foreignKeys.get(i).getFktable_db());
        final String fkTableName = normalizeIdentifier(foreignKeys.get(i).getFktable_name());
        // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
        // For instance, this is the case when we are creating the table.
        final ObjectStore.AttachedMTableInfo nChildTable = getMTable(catName, fkTableDB, fkTableName, retrieveCD);
        final MTable childTable = nChildTable.mtbl;
        if (childTable == null) {
          throw new InvalidObjectException("Child table not found: " + fkTableName);
        }
        MColumnDescriptor childCD = retrieveCD ? nChildTable.mcd : childTable.getSd().getCD();
        final List<MFieldSchema> childCols = childCD == null || childCD.getCols() == null ?
            new ArrayList<>() : new ArrayList<>(childCD.getCols());
        if (childTable.getPartitionKeys() != null) {
          childCols.addAll(childTable.getPartitionKeys());
        }

        final String pkTableDB = normalizeIdentifier(foreignKeys.get(i).getPktable_db());
        final String pkTableName = normalizeIdentifier(foreignKeys.get(i).getPktable_name());
        // For primary keys, we retrieve the column descriptors if retrieveCD is true (which means
        // it is an alter table statement) or if it is a create table statement but we are
        // referencing another table instead of self for the primary key.
        final ObjectStore.AttachedMTableInfo nParentTable;
        final MTable parentTable;
        MColumnDescriptor parentCD;
        final List<MFieldSchema> parentCols;
        final List<SQLPrimaryKey> existingTablePrimaryKeys;
        final List<SQLUniqueConstraint> existingTableUniqueConstraints;
        final boolean sameTable = fkTableDB.equals(pkTableDB) && fkTableName.equals(pkTableName);
        if (sameTable) {
          nParentTable = nChildTable;
          parentTable = childTable;
          parentCD = childCD;
          parentCols = childCols;
          existingTablePrimaryKeys = primaryKeys;
          existingTableUniqueConstraints = uniqueConstraints;
        } else {
          nParentTable = getMTable(catName, pkTableDB, pkTableName, true);
          parentTable = nParentTable.mtbl;
          if (parentTable == null) {
            throw new InvalidObjectException("Parent table not found: " + pkTableName);
          }
          parentCD = nParentTable.mcd;
          parentCols = parentCD == null || parentCD.getCols() == null ?
              new ArrayList<>() : new ArrayList<>(parentCD.getCols());
          if (parentTable.getPartitionKeys() != null) {
            parentCols.addAll(parentTable.getPartitionKeys());
          }
          PrimaryKeysRequest primaryKeysRequest = new PrimaryKeysRequest(pkTableDB, pkTableName);
          primaryKeysRequest.setCatName(catName);
          existingTablePrimaryKeys = getPrimaryKeys(primaryKeysRequest);
          existingTableUniqueConstraints =
              getUniqueConstraints(new UniqueConstraintsRequest(catName, pkTableDB, pkTableName));
        }

        // Here we build an aux structure that is used to verify that the foreign key that is declared
        // is actually referencing a valid primary key or unique key. We also check that the types of
        // the columns correspond.
        if (existingTablePrimaryKeys.isEmpty() && existingTableUniqueConstraints.isEmpty()) {
          throw new MetaException(
              "Trying to define foreign key but there are no primary keys or unique keys for referenced table");
        }
        final Set<String> validPKsOrUnique = generateValidPKsOrUniqueSignatures(parentCols,
            existingTablePrimaryKeys, existingTableUniqueConstraints);

        StringBuilder fkSignature = new StringBuilder();
        StringBuilder referencedKSignature = new StringBuilder();
        for (; i < foreignKeys.size(); i++) {
          SQLForeignKey foreignKey = foreignKeys.get(i);
          final String fkColumnName = normalizeIdentifier(foreignKey.getFkcolumn_name());
          int childIntegerIndex = getColumnIndexFromTableColumns(childCD.getCols(), fkColumnName);
          if (childIntegerIndex == -1) {
            if (childTable.getPartitionKeys() != null) {
              childCD = null;
              childIntegerIndex = getColumnIndexFromTableColumns(childTable.getPartitionKeys(), fkColumnName);
            }
            if (childIntegerIndex == -1) {
              throw new InvalidObjectException("Child column not found: " + fkColumnName);
            }
          }

          final String pkColumnName = normalizeIdentifier(foreignKey.getPkcolumn_name());
          int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD.getCols(), pkColumnName);
          if (parentIntegerIndex == -1) {
            if (parentTable.getPartitionKeys() != null) {
              parentCD = null;
              parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), pkColumnName);
            }
            if (parentIntegerIndex == -1) {
              throw new InvalidObjectException("Parent column not found: " + pkColumnName);
            }
          }

          if (foreignKey.getFk_name() == null) {
            // When there is no explicit foreign key name associated with the constraint and the key is composite,
            // we expect the foreign keys to be send in order in the input list.
            // Otherwise, the below code will break.
            // If this is the first column of the FK constraint, generate the foreign key name
            // NB: The below code can result in race condition where duplicate names can be generated (in theory).
            // However, this scenario can be ignored for practical purposes because of
            // the uniqueness of the generated constraint name.
            if (foreignKey.getKey_seq() == 1) {
              currentConstraintName = generateConstraintName(parentTable, fkTableDB, fkTableName, pkTableDB,
                  pkTableName, pkColumnName, fkColumnName, "fk");
            }
          } else {
            currentConstraintName = normalizeIdentifier(foreignKey.getFk_name());
            if (constraintNameAlreadyExists(parentTable, currentConstraintName)) {
              String fqConstraintName = String.format("%s.%s.%s", parentTable.getDatabase().getName(),
                  parentTable.getTableName(), currentConstraintName);
              throw new InvalidObjectException("Constraint name already exists: " + fqConstraintName);
            }
          }
          // Update Column, keys, table, database, catalog name
          foreignKey.setFk_name(currentConstraintName);
          foreignKey.setCatName(catName);
          foreignKey.setFktable_db(fkTableDB);
          foreignKey.setFktable_name(fkTableName);
          foreignKey.setPktable_db(pkTableDB);
          foreignKey.setPktable_name(pkTableName);
          foreignKey.setFkcolumn_name(fkColumnName);
          foreignKey.setPkcolumn_name(pkColumnName);

          Integer updateRule = foreignKey.getUpdate_rule();
          Integer deleteRule = foreignKey.getDelete_rule();
          int enableValidateRely = (foreignKey.isEnable_cstr() ? 4 : 0) +
              (foreignKey.isValidate_cstr() ? 2 : 0) + (foreignKey.isRely_cstr() ? 1 : 0);

          MConstraint mpkfk = new MConstraint(
              currentConstraintName,
              foreignKey.getKey_seq(),
              MConstraint.FOREIGN_KEY_CONSTRAINT,
              deleteRule,
              updateRule,
              enableValidateRely,
              parentTable,
              childTable,
              parentCD,
              childCD,
              childIntegerIndex,
              parentIntegerIndex
          );
          mpkfks.add(mpkfk);

          final String fkColType = getColumnFromTableColumns(childCols, fkColumnName).getType();
          fkSignature.append(
              generateColNameTypeSignature(fkColumnName, fkColType));
          referencedKSignature.append(
              generateColNameTypeSignature(pkColumnName, fkColType));

          if (i + 1 < foreignKeys.size() && foreignKeys.get(i + 1).getKey_seq() == 1) {
            // Next one is a new key, we bail out from the inner loop
            break;
          }
        }
        String referenced = referencedKSignature.toString();
        if (!validPKsOrUnique.contains(referenced)) {
          throw new MetaException(
              "Foreign key references " + referenced + " but no corresponding "
                  + "primary key or unique key exists. Possible keys: " + validPKsOrUnique);
        }
        if (sameTable && fkSignature.toString().equals(referenced)) {
          throw new MetaException(
              "Cannot be both foreign key and primary/unique key on same table: " + referenced);
        }
        fkSignature = new StringBuilder();
        referencedKSignature = new StringBuilder();
      }
      pm.makePersistentAll(mpkfks);

    }
    return foreignKeys;
  }

  private static Set<String> generateValidPKsOrUniqueSignatures(List<MFieldSchema> tableCols,
      List<SQLPrimaryKey> refTablePrimaryKeys, List<SQLUniqueConstraint> refTableUniqueConstraints) {
    final Set<String> validPKsOrUnique = new HashSet<>();
    if (!refTablePrimaryKeys.isEmpty()) {
      refTablePrimaryKeys.sort((o1, o2) -> {
        int keyNameComp = o1.getPk_name().compareTo(o2.getPk_name());
        if (keyNameComp == 0) {
          return Integer.compare(o1.getKey_seq(), o2.getKey_seq());
        }
        return keyNameComp;
      });
      StringBuilder pkSignature = new StringBuilder();
      for (SQLPrimaryKey pk : refTablePrimaryKeys) {
        pkSignature.append(
            generateColNameTypeSignature(
                pk.getColumn_name(), getColumnFromTableColumns(tableCols, pk.getColumn_name()).getType()));
      }
      validPKsOrUnique.add(pkSignature.toString());
    }
    if (!refTableUniqueConstraints.isEmpty()) {
      refTableUniqueConstraints.sort((o1, o2) -> {
        int keyNameComp = o1.getUk_name().compareTo(o2.getUk_name());
        if (keyNameComp == 0) {
          return Integer.compare(o1.getKey_seq(), o2.getKey_seq());
        }
        return keyNameComp;
      });
      StringBuilder ukSignature = new StringBuilder();
      for (int j = 0; j < refTableUniqueConstraints.size(); j++) {
        SQLUniqueConstraint uk = refTableUniqueConstraints.get(j);
        ukSignature.append(
            generateColNameTypeSignature(
                uk.getColumn_name(), getColumnFromTableColumns(tableCols, uk.getColumn_name()).getType()));
        if (j + 1 < refTableUniqueConstraints.size()) {
          if (!refTableUniqueConstraints.get(j + 1).getUk_name().equals(
              refTableUniqueConstraints.get(j).getUk_name())) {
            validPKsOrUnique.add(ukSignature.toString());
            ukSignature = new StringBuilder();
          }
        } else {
          validPKsOrUnique.add(ukSignature.toString());
        }
      }
    }
    return validPKsOrUnique;
  }

  private static String generateColNameTypeSignature(String colName, String colType) {
    return colName + ":" + colType + ";";
  }

  @Override
  public List<SQLPrimaryKey> addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException,
      MetaException {
    return addPrimaryKeys(pks, true);
  }

  private List<SQLPrimaryKey> addPrimaryKeys(List<SQLPrimaryKey> pks, boolean retrieveCD) throws InvalidObjectException,
      MetaException {
    List<MConstraint> mpks = new ArrayList<>();
    String constraintName = null;

    for (SQLPrimaryKey pk : pks) {
      final String catName = normalizeIdentifier(pk.getCatName());
      final String tableDB = normalizeIdentifier(pk.getTable_db());
      final String tableName = normalizeIdentifier(pk.getTable_name());
      final String columnName = normalizeIdentifier(pk.getColumn_name());

      // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
      // For instance, this is the case when we are creating the table.
      ObjectStore.AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + tableName);
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
      if (parentIntegerIndex == -1) {
        if (parentTable.getPartitionKeys() != null) {
          parentCD = null;
          parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
        }
        if (parentIntegerIndex == -1) {
          throw new InvalidObjectException("Parent column not found: " + columnName);
        }
      }
      if (getPrimaryKeyConstraintName(parentTable.getDatabase().getCatalogName(),
          parentTable.getDatabase().getName(), parentTable.getTableName()) != null) {
        throw new MetaException(" Primary key already exists for: " +
            TableName.getQualified(catName, tableDB, tableName));
      }
      if (pk.getPk_name() == null) {
        if (pk.getKey_seq() == 1) {
          constraintName = generateConstraintName(parentTable, tableDB, tableName, columnName, "pk");
        }
      } else {
        constraintName = normalizeIdentifier(pk.getPk_name());
        if (constraintNameAlreadyExists(parentTable, constraintName)) {
          String fqConstraintName = String.format("%s.%s.%s", parentTable.getDatabase().getName(),
              parentTable.getTableName(), constraintName);
          throw new InvalidObjectException("Constraint name already exists: " + fqConstraintName);
        }
      }

      int enableValidateRely = (pk.isEnable_cstr() ? 4 : 0) +
          (pk.isValidate_cstr() ? 2 : 0) + (pk.isRely_cstr() ? 1 : 0);
      MConstraint mpk = new MConstraint(
          constraintName,
          pk.getKey_seq(),
          MConstraint.PRIMARY_KEY_CONSTRAINT,
          null,
          null,
          enableValidateRely,
          parentTable,
          null,
          parentCD,
          null,
          null,
          parentIntegerIndex);
      mpks.add(mpk);

      // Add normalized identifier back to result
      pk.setCatName(catName);
      pk.setTable_db(tableDB);
      pk.setTable_name(tableName);
      pk.setColumn_name(columnName);
      pk.setPk_name(constraintName);
    }
    pm.makePersistentAll(mpks);
    return pks;
  }

  @Override
  public List<SQLUniqueConstraint> addUniqueConstraints(List<SQLUniqueConstraint> uks)
      throws InvalidObjectException, MetaException {
    return addUniqueConstraints(uks, true);
  }

  private List<SQLUniqueConstraint> addUniqueConstraints(List<SQLUniqueConstraint> uks, boolean retrieveCD)
      throws InvalidObjectException, MetaException {

    List<MConstraint> cstrs = new ArrayList<>();
    String constraintName = null;

    for (SQLUniqueConstraint uk : uks) {
      final String catName = normalizeIdentifier(uk.getCatName());
      final String tableDB = normalizeIdentifier(uk.getTable_db());
      final String tableName = normalizeIdentifier(uk.getTable_name());
      final String columnName = normalizeIdentifier(uk.getColumn_name());

      // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
      // For instance, this is the case when we are creating the table.
      ObjectStore.AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + tableName);
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
      if (parentIntegerIndex == -1) {
        if (parentTable.getPartitionKeys() != null) {
          parentCD = null;
          parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
        }
        if (parentIntegerIndex == -1) {
          throw new InvalidObjectException("Parent column not found: " + columnName);
        }
      }
      if (uk.getUk_name() == null) {
        if (uk.getKey_seq() == 1) {
          constraintName = generateConstraintName(parentTable, tableDB, tableName, columnName, "uk");
        }
      } else {
        constraintName = normalizeIdentifier(uk.getUk_name());
        if (constraintNameAlreadyExists(parentTable, constraintName)) {
          String fqConstraintName = String.format("%s.%s.%s", parentTable.getDatabase().getName(),
              parentTable.getTableName(), constraintName);
          throw new InvalidObjectException("Constraint name already exists: " + fqConstraintName);
        }
      }


      int enableValidateRely = (uk.isEnable_cstr() ? 4 : 0) +
          (uk.isValidate_cstr() ? 2 : 0) + (uk.isRely_cstr() ? 1 : 0);
      MConstraint muk = new MConstraint(
          constraintName,
          uk.getKey_seq(),
          MConstraint.UNIQUE_CONSTRAINT,
          null,
          null,
          enableValidateRely,
          parentTable,
          null,
          parentCD,
          null,
          null,
          parentIntegerIndex);
      cstrs.add(muk);

      // Add normalized identifier back to result
      uk.setCatName(catName);
      uk.setTable_db(tableDB);
      uk.setTable_name(tableName);
      uk.setColumn_name(columnName);
      uk.setUk_name(constraintName);

    }
    pm.makePersistentAll(cstrs);
    return uks;
  }

  @Override
  public List<SQLNotNullConstraint> addNotNullConstraints(List<SQLNotNullConstraint> nns)
      throws InvalidObjectException, MetaException {
    return addNotNullConstraints(nns, true);
  }

  @Override
  public List<SQLDefaultConstraint> addDefaultConstraints(List<SQLDefaultConstraint> nns)
      throws InvalidObjectException, MetaException {
    return addDefaultConstraints(nns, true);
  }

  @Override
  public List<SQLCheckConstraint> addCheckConstraints(List<SQLCheckConstraint> nns)
      throws InvalidObjectException, MetaException {
    return addCheckConstraints(nns, true);
  }

  private List<SQLCheckConstraint> addCheckConstraints(List<SQLCheckConstraint> ccs, boolean retrieveCD)
      throws InvalidObjectException, MetaException {
    List<MConstraint> cstrs = new ArrayList<>();

    for (SQLCheckConstraint cc: ccs) {
      final String catName = normalizeIdentifier(cc.getCatName());
      final String tableDB = normalizeIdentifier(cc.getTable_db());
      final String tableName = normalizeIdentifier(cc.getTable_name());
      final String columnName = cc.getColumn_name() == null? null
          : normalizeIdentifier(cc.getColumn_name());
      final String ccName = cc.getDc_name();
      boolean isEnable = cc.isEnable_cstr();
      boolean isValidate = cc.isValidate_cstr();
      boolean isRely = cc.isRely_cstr();
      String constraintValue = cc.getCheck_expression();
      MConstraint muk = addConstraint(catName, tableDB, tableName, columnName, ccName, isEnable, isRely, isValidate,
          MConstraint.CHECK_CONSTRAINT, constraintValue, retrieveCD);
      cstrs.add(muk);

      // Add normalized identifier back to result
      cc.setCatName(catName);
      cc.setTable_db(tableDB);
      cc.setTable_name(tableName);
      cc.setColumn_name(columnName);
      cc.setDc_name(muk.getConstraintName());
    }
    pm.makePersistentAll(cstrs);
    return ccs;
  }

  private MConstraint addConstraint(String catName, String tableDB, String tableName, String columnName, String ccName,
      boolean isEnable, boolean isRely, boolean isValidate, int constraintType,
      String constraintValue, boolean retrieveCD)
      throws InvalidObjectException, MetaException {
    String constraintName = null;
    // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
    // For instance, this is the case when we are creating the table.
    ObjectStore.AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
    MTable parentTable = nParentTable.mtbl;
    if (parentTable == null) {
      throw new InvalidObjectException("Parent table not found: " + tableName);
    }

    MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
    int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
    if (parentIntegerIndex == -1) {
      if (parentTable.getPartitionKeys() != null) {
        parentCD = null;
        parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
      }
    }
    if (ccName == null) {
      constraintName = generateConstraintName(parentTable, tableDB, tableName, columnName, "dc");
    } else {
      constraintName = normalizeIdentifier(ccName);
      if (constraintNameAlreadyExists(parentTable, constraintName)) {
        String fqConstraintName = String.format("%s.%s.%s", parentTable.getDatabase().getName(),
            parentTable.getTableName(), constraintName);
        throw new InvalidObjectException("Constraint name already exists: " + fqConstraintName);
      }
    }

    int enableValidateRely = (isEnable ? 4 : 0) +
        (isValidate ? 2 : 0) + (isRely ? 1 : 0);
    MConstraint muk = new MConstraint(
        constraintName,
        1,
        constraintType, // Not null constraint should reference a single column
        null,
        null,
        enableValidateRely,
        parentTable,
        null,
        parentCD,
        null,
        null,
        parentIntegerIndex,
        constraintValue);

    return muk;
  }

  private List<SQLDefaultConstraint> addDefaultConstraints(List<SQLDefaultConstraint> dcs, boolean retrieveCD)
      throws InvalidObjectException, MetaException {

    List<MConstraint> cstrs = new ArrayList<>();
    for (SQLDefaultConstraint dc : dcs) {
      final String catName = normalizeIdentifier(dc.getCatName());
      final String tableDB = normalizeIdentifier(dc.getTable_db());
      final String tableName = normalizeIdentifier(dc.getTable_name());
      final String columnName = normalizeIdentifier(dc.getColumn_name());
      final String dcName = dc.getDc_name();
      boolean isEnable = dc.isEnable_cstr();
      boolean isValidate = dc.isValidate_cstr();
      boolean isRely = dc.isRely_cstr();
      String constraintValue = dc.getDefault_value();
      MConstraint muk = addConstraint(catName, tableDB, tableName, columnName, dcName, isEnable, isRely, isValidate,
          MConstraint.DEFAULT_CONSTRAINT, constraintValue, retrieveCD);
      cstrs.add(muk);

      // Add normalized identifier back to result
      dc.setCatName(catName);
      dc.setTable_db(tableDB);
      dc.setTable_name(tableName);
      dc.setColumn_name(columnName);
      dc.setDc_name(muk.getConstraintName());
    }
    pm.makePersistentAll(cstrs);
    return dcs;
  }

  private List<SQLNotNullConstraint> addNotNullConstraints(List<SQLNotNullConstraint> nns, boolean retrieveCD)
      throws InvalidObjectException, MetaException {

    List<MConstraint> cstrs = new ArrayList<>();
    String constraintName;

    for (SQLNotNullConstraint nn : nns) {
      final String catName = normalizeIdentifier(nn.getCatName());
      final String tableDB = normalizeIdentifier(nn.getTable_db());
      final String tableName = normalizeIdentifier(nn.getTable_name());
      final String columnName = normalizeIdentifier(nn.getColumn_name());

      // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
      // For instance, this is the case when we are creating the table.
      ObjectStore.AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + tableName);
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
      if (parentIntegerIndex == -1) {
        if (parentTable.getPartitionKeys() != null) {
          parentCD = null;
          parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
        }
        if (parentIntegerIndex == -1) {
          throw new InvalidObjectException("Parent column not found: " + columnName);
        }
      }
      if (nn.getNn_name() == null) {
        constraintName = generateConstraintName(parentTable, tableDB, tableName, columnName, "nn");
      } else {
        constraintName = normalizeIdentifier(nn.getNn_name());
        if (constraintNameAlreadyExists(parentTable, constraintName)) {
          String fqConstraintName = String.format("%s.%s.%s", parentTable.getDatabase().getName(),
              parentTable.getTableName(), constraintName);
          throw new InvalidObjectException("Constraint name already exists: " + fqConstraintName);
        }
      }

      int enableValidateRely = (nn.isEnable_cstr() ? 4 : 0) +
          (nn.isValidate_cstr() ? 2 : 0) + (nn.isRely_cstr() ? 1 : 0);
      MConstraint muk = new MConstraint(
          constraintName,
          1,
          MConstraint.NOT_NULL_CONSTRAINT, // Not null constraint should reference a single column
          null,
          null,
          enableValidateRely,
          parentTable,
          null,
          parentCD,
          null,
          null,
          parentIntegerIndex);
      cstrs.add(muk);
      // Add normalized identifier back to result
      nn.setCatName(catName);
      nn.setTable_db(tableDB);
      nn.setTable_name(tableName);
      nn.setColumn_name(columnName);
      nn.setNn_name(constraintName);
    }
    pm.makePersistentAll(cstrs);
    return nns;
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request) throws MetaException {
    try {
      return getPrimaryKeysInternal(request.getCatName(),
          request.getDb_name(),request.getTbl_name());
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLPrimaryKey> getPrimaryKeysInternal(final String catName,
      final String dbNameInput,
      final String tblNameInput)
      throws MetaException, NoSuchObjectException {
    final String dbName = dbNameInput != null ? normalizeIdentifier(dbNameInput) : null;
    final String tblName = normalizeIdentifier(tblNameInput);
    return new ObjectStore.GetListHelper<SQLPrimaryKey>(catName, dbName, tblName, true, true) {

      @Override
      protected List<SQLPrimaryKey> getSqlResult(ObjectStore.GetHelper<List<SQLPrimaryKey>> ctx) throws MetaException {
        return directSql.getPrimaryKeys(catName, dbName, tblName);
      }

      @Override
      protected List<SQLPrimaryKey> getJdoResult(
          ObjectStore.GetHelper<List<SQLPrimaryKey>> ctx) throws MetaException, NoSuchObjectException {
        return getPrimaryKeysViaJdo(catName, dbName, tblName);
      }
    }.run(false);
  }

  private List<SQLPrimaryKey> getPrimaryKeysViaJdo(String catName, String dbName, String tblName) {
    List<SQLPrimaryKey> primaryKeys = null;
    Query query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
            + " parentTable.database.catalogName == cat_name &&"
            + " constraintType == MConstraint.PRIMARY_KEY_CONSTRAINT");
    query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, " +
        "java.lang.String cat_name");
    Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
    pm.retrieveAll(constraints);
    primaryKeys = new ArrayList<>();
    for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
      MConstraint currPK = (MConstraint) i.next();
      List<MFieldSchema> cols = currPK.getParentColumn() != null ?
          currPK.getParentColumn().getCols() : currPK.getParentTable().getPartitionKeys();
      int enableValidateRely = currPK.getEnableValidateRely();
      boolean enable = (enableValidateRely & 4) != 0;
      boolean validate = (enableValidateRely & 2) != 0;
      boolean rely = (enableValidateRely & 1) != 0;
      SQLPrimaryKey keyCol = new SQLPrimaryKey(dbName,
          tblName,
          cols.get(currPK.getParentIntegerIndex()).getName(),
          currPK.getPosition(),
          currPK.getConstraintName(), enable, validate, rely);
      keyCol.setCatName(catName);
      primaryKeys.add(keyCol);
    }
    return primaryKeys;
  }

  private String getPrimaryKeyConstraintName(String catName, String dbName, String tblName) {
    String ret = null;
    Query query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
            + " parentTable.database.catalogName == catName &&"
            + " constraintType == MConstraint.PRIMARY_KEY_CONSTRAINT");
    query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, " +
        "java.lang.String catName");
    Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
    pm.retrieveAll(constraints);
    for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
      MConstraint currPK = (MConstraint) i.next();
      ret = currPK.getConstraintName();
      break;
    }
    return ret;
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request) throws MetaException {
    try {
      return getForeignKeysInternal(request.getCatName(),
          request.getParent_db_name(), request.getParent_tbl_name() ,
          request.getForeign_db_name(),request.getForeign_tbl_name(), true,
          true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLForeignKey> getForeignKeysInternal(
      final String catName, final String parent_db_name_input, final String parent_tbl_name_input,
      final String foreign_db_name_input, final String foreign_tbl_name_input, boolean allowSql,
      boolean allowJdo) throws MetaException, NoSuchObjectException {
    final String parent_db_name = (parent_db_name_input != null) ? normalizeIdentifier(parent_db_name_input) : null;
    final String parent_tbl_name = (parent_tbl_name_input != null) ? normalizeIdentifier(parent_tbl_name_input) : null;
    final String foreign_db_name = (foreign_db_name_input != null) ? normalizeIdentifier(foreign_db_name_input) : null;
    final String foreign_tbl_name = (foreign_tbl_name_input != null)
        ? normalizeIdentifier(foreign_tbl_name_input) : null;
    final String db_name;
    final String tbl_name;
    if (foreign_tbl_name == null) {
      // The FK table name might be null if we are retrieving the constraint from the PK side
      db_name = parent_db_name;
      tbl_name = parent_tbl_name;
    } else {
      db_name = foreign_db_name;
      tbl_name = foreign_tbl_name;
    }
    return new ObjectStore.GetListHelper<SQLForeignKey>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLForeignKey> getSqlResult(ObjectStore.GetHelper<List<SQLForeignKey>> ctx) throws MetaException {
        return directSql.getForeignKeys(catName, parent_db_name,
            parent_tbl_name, foreign_db_name, foreign_tbl_name);
      }

      @Override
      protected List<SQLForeignKey> getJdoResult(
          ObjectStore.GetHelper<List<SQLForeignKey>> ctx) throws MetaException, NoSuchObjectException {
        return getForeignKeysViaJdo(catName, parent_db_name,
            parent_tbl_name, foreign_db_name, foreign_tbl_name);
      }
    }.run(false);
  }

  private List<SQLForeignKey> getForeignKeysViaJdo(String catName, String parentDbName,
      String parentTblName, String foreignDbName, String foreignTblName) {
    List<SQLForeignKey> foreignKeys = null;
    Collection<?> constraints = null;
    Map<String, String> tblToConstraint = new HashMap<>();
    String queryText =
        " parentTable.database.catalogName == catName1 &&" + "childTable.database.catalogName == catName2 && " + (
            parentTblName != null ? "parentTable.tableName == parent_tbl_name && " : "") + (
            parentDbName != null ? " parentTable.database.name == parent_db_name && " : "") + (
            foreignTblName != null ? " childTable.tableName == foreign_tbl_name && " : "") + (
            foreignDbName != null ? " childTable.database.name == foreign_db_name && " : "")
            + " constraintType == MConstraint.FOREIGN_KEY_CONSTRAINT";
    queryText = queryText.trim();
    Query query = pm.newQuery(MConstraint.class, queryText);
    String paramText = "java.lang.String catName1, java.lang.String catName2" + (
        parentTblName == null ? "" : ", java.lang.String parent_tbl_name") + (
        parentDbName == null ? "" : " , java.lang.String parent_db_name") + (
        foreignTblName == null ? "" : ", java.lang.String foreign_tbl_name") + (
        foreignDbName == null ? "" : " , java.lang.String foreign_db_name");
    query.declareParameters(paramText);
    List<String> params = new ArrayList<>();
    params.add(catName);
    params.add(catName); // This is not a mistake, catName is in the where clause twice
    if (parentTblName != null) {
      params.add(parentTblName);
    }
    if (parentDbName != null) {
      params.add(parentDbName);
    }
    if (foreignTblName != null) {
      params.add(foreignTblName);
    }
    if (foreignDbName != null) {
      params.add(foreignDbName);
    }
    constraints = (Collection<?>) query.executeWithArray(params.toArray(new String[0]));

    pm.retrieveAll(constraints);
    foreignKeys = new ArrayList<>();
    for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
      MConstraint currPKFK = (MConstraint) i.next();
      List<MFieldSchema> parentCols = currPKFK.getParentColumn() != null ?
          currPKFK.getParentColumn().getCols() : currPKFK.getParentTable().getPartitionKeys();
      List<MFieldSchema> childCols = currPKFK.getChildColumn() != null ?
          currPKFK.getChildColumn().getCols() : currPKFK.getChildTable().getPartitionKeys();
      int enableValidateRely = currPKFK.getEnableValidateRely();
      boolean enable = (enableValidateRely & 4) != 0;
      boolean validate = (enableValidateRely & 2) != 0;
      boolean rely = (enableValidateRely & 1) != 0;
      String consolidatedtblName =
          currPKFK.getParentTable().getDatabase().getName() + "." +
              currPKFK.getParentTable().getTableName();
      String pkName;
      if (tblToConstraint.containsKey(consolidatedtblName)) {
        pkName = tblToConstraint.get(consolidatedtblName);
      } else {
        pkName = getPrimaryKeyConstraintName(currPKFK.getParentTable().getDatabase().getCatalogName(),
            currPKFK.getParentTable().getDatabase().getName(),
            currPKFK.getParentTable().getTableName());
        tblToConstraint.put(consolidatedtblName, pkName);
      }
      SQLForeignKey fk = new SQLForeignKey(
          currPKFK.getParentTable().getDatabase().getName(),
          currPKFK.getParentTable().getTableName(),
          parentCols.get(currPKFK.getParentIntegerIndex()).getName(),
          currPKFK.getChildTable().getDatabase().getName(),
          currPKFK.getChildTable().getTableName(),
          childCols.get(currPKFK.getChildIntegerIndex()).getName(),
          currPKFK.getPosition(),
          currPKFK.getUpdateRule(),
          currPKFK.getDeleteRule(),
          currPKFK.getConstraintName(), pkName, enable, validate, rely);
      fk.setCatName(catName);
      foreignKeys.add(fk);
    }
    return foreignKeys;
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request) throws MetaException {
    try {
      return getUniqueConstraintsInternal(request.getCatName(),
          request.getDb_name(),request.getTbl_name(), true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLUniqueConstraint> getUniqueConstraintsInternal(
      String catNameInput, final String db_name_input, final String tbl_name_input,
      boolean allowSql, boolean allowJdo) throws MetaException, NoSuchObjectException {
    final String catName = normalizeIdentifier(catNameInput);
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new ObjectStore.GetListHelper<SQLUniqueConstraint>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLUniqueConstraint> getSqlResult(ObjectStore.GetHelper<List<SQLUniqueConstraint>> ctx)
          throws MetaException {
        return directSql.getUniqueConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLUniqueConstraint> getJdoResult(ObjectStore.GetHelper<List<SQLUniqueConstraint>> ctx)
          throws MetaException, NoSuchObjectException {
        return getUniqueConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLUniqueConstraint> getUniqueConstraintsViaJdo(String catName, String dbName, String tblName) {
    List<SQLUniqueConstraint> uniqueConstraints = null;
    Query query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name && parentTable.database.catalogName == catName &&"
            + " constraintType == MConstraint.UNIQUE_CONSTRAINT");
    query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
    Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
    pm.retrieveAll(constraints);
    uniqueConstraints = new ArrayList<>();
    for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
      MConstraint currConstraint = (MConstraint) i.next();
      List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
          currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
      int enableValidateRely = currConstraint.getEnableValidateRely();
      boolean enable = (enableValidateRely & 4) != 0;
      boolean validate = (enableValidateRely & 2) != 0;
      boolean rely = (enableValidateRely & 1) != 0;
      uniqueConstraints.add(new SQLUniqueConstraint(catName, dbName, tblName,
          cols.get(currConstraint.getParentIntegerIndex()).getName(), currConstraint.getPosition(),
          currConstraint.getConstraintName(), enable, validate, rely));
    }
    return uniqueConstraints;
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request) throws MetaException {
    try {
      return getNotNullConstraintsInternal(request.getCatName(),request.getDb_name(),request.getTbl_name(), true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request) throws MetaException {
    try {
      return getDefaultConstraintsInternal(request.getCatName(),request.getDb_name(),request.getTbl_name(), true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request) throws MetaException {
    try {
      return getCheckConstraintsInternal(request.getCatName(),request.getDb_name(),request.getTbl_name(), true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLDefaultConstraint> getDefaultConstraintsInternal(
      String catName, final String db_name_input, final String tbl_name_input, boolean allowSql,
      boolean allowJdo) throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new ObjectStore.GetListHelper<SQLDefaultConstraint>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLDefaultConstraint> getSqlResult(ObjectStore.GetHelper<List<SQLDefaultConstraint>> ctx)
          throws MetaException {
        return directSql.getDefaultConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLDefaultConstraint> getJdoResult(ObjectStore.GetHelper<List<SQLDefaultConstraint>> ctx)
          throws MetaException, NoSuchObjectException {
        return getDefaultConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  protected List<SQLCheckConstraint> getCheckConstraintsInternal(String catName, final String db_name_input,
      final String tbl_name_input, boolean allowSql,
      boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new ObjectStore.GetListHelper<SQLCheckConstraint>(normalizeIdentifier(catName), db_name, tbl_name,
        allowSql, allowJdo) {

      @Override
      protected List<SQLCheckConstraint> getSqlResult(ObjectStore.GetHelper<List<SQLCheckConstraint>> ctx)
          throws MetaException {
        return directSql.getCheckConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLCheckConstraint> getJdoResult(ObjectStore.GetHelper<List<SQLCheckConstraint>> ctx)
          throws MetaException, NoSuchObjectException {
        return getCheckConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLCheckConstraint> getCheckConstraintsViaJdo(String catName, String dbName, String tblName) {
    List<SQLCheckConstraint> checkConstraints= null;
    Query query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
            + " parentTable.database.catalogName == catName && constraintType == MConstraint.CHECK_CONSTRAINT");
    query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
    Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
    pm.retrieveAll(constraints);
    checkConstraints = new ArrayList<>();
    for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
      MConstraint currConstraint = (MConstraint) i.next();
      List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
          currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
      int enableValidateRely = currConstraint.getEnableValidateRely();
      boolean enable = (enableValidateRely & 4) != 0;
      boolean validate = (enableValidateRely & 2) != 0;
      boolean rely = (enableValidateRely & 1) != 0;
      checkConstraints.add(new SQLCheckConstraint(catName, dbName, tblName,
          cols.get(currConstraint.getParentIntegerIndex()).getName(),
          currConstraint.getDefaultValue(),
          currConstraint.getConstraintName(), enable, validate, rely));
    }
    return checkConstraints;
  }

  private List<SQLDefaultConstraint> getDefaultConstraintsViaJdo(String catName, String dbName, String tblName) {
    List<SQLDefaultConstraint> defaultConstraints= null;
    Query query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
            + " parentTable.database.catalogName == catName &&"
            + " constraintType == MConstraint.DEFAULT_CONSTRAINT");
    query.declareParameters(
        "java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
    Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
    pm.retrieveAll(constraints);
    defaultConstraints = new ArrayList<>();
    for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
      MConstraint currConstraint = (MConstraint) i.next();
      List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
          currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
      int enableValidateRely = currConstraint.getEnableValidateRely();
      boolean enable = (enableValidateRely & 4) != 0;
      boolean validate = (enableValidateRely & 2) != 0;
      boolean rely = (enableValidateRely & 1) != 0;
      defaultConstraints.add(new SQLDefaultConstraint(catName, dbName, tblName,
          cols.get(currConstraint.getParentIntegerIndex()).getName(), currConstraint.getDefaultValue(),
          currConstraint.getConstraintName(), enable, validate, rely));
    }
    return defaultConstraints;
  }

  protected List<SQLNotNullConstraint> getNotNullConstraintsInternal(String catName, final String db_name_input,
      final String tbl_name_input, boolean allowSql, boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new ObjectStore.GetListHelper<SQLNotNullConstraint>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLNotNullConstraint> getSqlResult(ObjectStore.GetHelper<List<SQLNotNullConstraint>> ctx)
          throws MetaException {
        return directSql.getNotNullConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLNotNullConstraint> getJdoResult(ObjectStore.GetHelper<List<SQLNotNullConstraint>> ctx)
          throws MetaException, NoSuchObjectException {
        return getNotNullConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLNotNullConstraint> getNotNullConstraintsViaJdo(String catName, String dbName, String tblName) {
    List<SQLNotNullConstraint> notNullConstraints = null;
    Query query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
            + " parentTable.database.catalogName == catName && constraintType == MConstraint.NOT_NULL_CONSTRAINT");
    query.declareParameters(
        "java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
    Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
    pm.retrieveAll(constraints);
    notNullConstraints = new ArrayList<>();
    for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
      MConstraint currConstraint = (MConstraint) i.next();
      List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
          currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
      int enableValidateRely = currConstraint.getEnableValidateRely();
      boolean enable = (enableValidateRely & 4) != 0;
      boolean validate = (enableValidateRely & 2) != 0;
      boolean rely = (enableValidateRely & 1) != 0;
      notNullConstraints.add(new SQLNotNullConstraint(catName, dbName,
          tblName,
          cols.get(currConstraint.getParentIntegerIndex()).getName(),
          currConstraint.getConstraintName(), enable, validate, rely));
    }
    return notNullConstraints;
  }

  /**
   * Api to fetch all constraints at once
   * @param request request object
   * @return all table constraints
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  @Override
  public SQLAllTableConstraints getAllTableConstraints(AllTableConstraintsRequest request)
      throws MetaException, NoSuchObjectException {
    String catName = request.getCatName();
    String dbName = request.getDbName();
    String tblName = request.getTblName();
    SQLAllTableConstraints sqlAllTableConstraints = new SQLAllTableConstraints();
    PrimaryKeysRequest primaryKeysRequest = new PrimaryKeysRequest(dbName, tblName);
    primaryKeysRequest.setCatName(catName);
    sqlAllTableConstraints.setPrimaryKeys(getPrimaryKeys(primaryKeysRequest));
    ForeignKeysRequest foreignKeysRequest =
        new ForeignKeysRequest(null, null, dbName, tblName);
    foreignKeysRequest.setCatName(catName);
    sqlAllTableConstraints.setForeignKeys(getForeignKeys(foreignKeysRequest));
    sqlAllTableConstraints.
        setUniqueConstraints(getUniqueConstraints(new UniqueConstraintsRequest(catName, dbName, tblName)));
    sqlAllTableConstraints.
        setDefaultConstraints(getDefaultConstraints(new DefaultConstraintsRequest(catName, dbName, tblName)));
    sqlAllTableConstraints.
        setCheckConstraints(getCheckConstraints(new CheckConstraintsRequest(catName, dbName, tblName)));
    sqlAllTableConstraints.
        setNotNullConstraints(getNotNullConstraints(new NotNullConstraintsRequest(catName, dbName, tblName)));
    return sqlAllTableConstraints;
  }

  @Override
  public void dropConstraint(TableName tableName, String constraintName, boolean missingOk)
      throws NoSuchObjectException {
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    List<MConstraint> tabConstraints =
        listAllTableConstraintsWithOptionalConstraintName(catName,  dbName, tblName, constraintName);
    if (CollectionUtils.isNotEmpty(tabConstraints)) {
      pm.deletePersistentAll(tabConstraints);
    } else if (!missingOk) {
      throw new NoSuchObjectException("The constraint: " + constraintName +
          " does not exist for the associated table: " + dbName + "." + tblName);
    }
  }

  private static MFieldSchema getColumnFromTableColumns(List<MFieldSchema> cols, String col) {
    if (cols == null) {
      return null;
    }
    for (MFieldSchema mfs : cols) {
      if (mfs.getName().equalsIgnoreCase(col)) {
        return mfs;
      }
    }
    return null;
  }

  private static int getColumnIndexFromTableColumns(List<MFieldSchema> cols, String col) {
    if (cols == null) {
      return -1;
    }
    for (int i = 0; i < cols.size(); i++) {
      MFieldSchema mfs = cols.get(i);
      if (mfs.getName().equalsIgnoreCase(col)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public void setBaseStore(RawStore store) {
    super.setBaseStore(store);
    this.conf = store.getConf();
  }

  class AttachedMTableInfo {
    MTable mtbl;
    MColumnDescriptor mcd;

    public AttachedMTableInfo() {}

    public AttachedMTableInfo(MTable mtbl, MColumnDescriptor mcd) {
      this.mtbl = mtbl;
      this.mcd = mcd;
    }
  }

  private AttachedMTableInfo getMTable(String catName, String db, String table,
      boolean retrieveCD) {
    AttachedMTableInfo nmtbl = new AttachedMTableInfo();
    MTable mtbl = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      catName = normalizeIdentifier(Optional.ofNullable(catName).orElse(getDefaultCatalog(conf)));
      db = normalizeIdentifier(db);
      table = normalizeIdentifier(table);
      query = pm.newQuery(MTable.class,
          "tableName == table && database.name == db && database.catalogName == catname");
      query.declareParameters(
          "java.lang.String table, java.lang.String db, java.lang.String catname");
      query.setUnique(true);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Executing getMTable for {}",
            TableName.getQualified(catName, db, table));
      }
      mtbl = (MTable) query.execute(table, db, catName);
      pm.retrieve(mtbl);
      // Retrieving CD can be expensive and unnecessary, so do it only when required.
      if (mtbl != null && retrieveCD) {
        pm.retrieve(mtbl.getSd());
        pm.retrieveAll(mtbl.getSd().getCD());
        nmtbl.mcd = mtbl.getSd().getCD();
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    nmtbl.mtbl = mtbl;
    return nmtbl;
  }

  private MTable getMTable(String catName, String db, String table) {
    AttachedMTableInfo nmtbl = getMTable(catName, db, table, false);
    return nmtbl.mtbl;
  }
}
