package org.apache.hadoop.hive.metastore.metastore.iface;

import java.util.List;

import org.apache.hadoop.hive.common.TableName;
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
import org.apache.hadoop.hive.metastore.metastore.MetaDescriptor;
import org.apache.hadoop.hive.metastore.metastore.impl.ConstraintStoreImpl;

@MetaDescriptor(alias = "constraint", defaultImpl = ConstraintStoreImpl.class)
public interface ConstraintStore {
  /**
   * SQLPrimaryKey represents a single primary key column.
   * Since a table can have one or more primary keys ( in case of composite primary key ),
   * this method returns List&lt;SQLPrimaryKey&gt;
   * @param request primary key request
   * @return list of primary key columns or an empty list if the table does not have a primary key
   * @throws MetaException error accessing the RDBMS
   */
  List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request)
      throws MetaException;

  /**
   * SQLForeignKey represents a single foreign key column.
   * Since a table can have one or more foreign keys ( in case of composite foreign key ),
   * this method returns List&lt;SQLForeignKey&gt;
   * @param request ForeignKeysRequest object
   * @return List of all matching foreign key columns.  Note that if more than one foreign key
   * matches the arguments the results here will be all mixed together into a single list.
   * @throws MetaException error access the RDBMS.
   */
  List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request)
      throws MetaException;

  /**
   * SQLUniqueConstraint represents a single unique constraint column.
   * Since a table can have one or more unique constraint ( in case of composite unique constraint ),
   * this method returns List&lt;SQLUniqueConstraint&gt;
   * @param request UniqueConstraintsRequest object.
   * @return list of unique constraints
   * @throws MetaException error access the RDBMS.
   */
  List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request) throws MetaException;

  /**
   * SQLNotNullConstraint represents a single not null constraint column.
   * Since a table can have one or more not null constraint ( in case of composite not null constraint ),
   * this method returns List&lt;SQLNotNullConstraint&gt;
   * @param request NotNullConstraintsRequest object.
   * @return list of not null constraints
   * @throws MetaException error accessing the RDBMS.
   */
  List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request) throws MetaException;

  /**
   * SQLDefaultConstraint represents a single default constraint column.
   * Since a table can have one or more default constraint ( in case of composite default constraint ),
   * this method returns List&lt;SQLDefaultConstraint&gt;
   * @param request DefaultConstraintsRequest object.
   * @return list of default values defined on the table.
   * @throws MetaException error accessing the RDBMS
   */
  List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request) throws MetaException;

  /**
   * SQLCheckConstraint represents a single check constraint column.
   * Since a table can have one or more check constraint ( in case of composite check constraint ),
   * this method returns List&lt;SQLCheckConstraint&gt;
   * @param request CheckConstraintsRequest object.
   * @return ccheck constraints for this table
   * @throws MetaException error accessing the RDBMS
   */
  List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request) throws MetaException;

  /**
   * Get table constraints
   * @param request AllTableConstraintsRequest object
   * @return all constraints for this table
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  SQLAllTableConstraints getAllTableConstraints(AllTableConstraintsRequest request)
      throws MetaException, NoSuchObjectException;

  /**
   * Create a table with constraints
   * @param tbl table definition
   * @param constraints wrapper of all table constraints
   * @return list of constraint names
   * @throws InvalidObjectException one of the provided objects is malformed.
   * @throws MetaException error accessing the RDBMS
   */
  SQLAllTableConstraints createTableWithConstraints(Table tbl, SQLAllTableConstraints constraints) throws InvalidObjectException, MetaException;

  /**
   * Drop a constraint, any constraint.  I have no idea why add and get each have separate
   * methods for each constraint type but drop has only one.
   * @param tableName table name
   * @param constraintName name of the constraint
   * @param missingOk if true, it is not an error if there is no constraint of this name.  If
   *                  false and there is no constraint of this name an exception will be thrown.
   * @throws NoSuchObjectException no constraint of this name exists and missingOk = false
   */
  void dropConstraint(TableName tableName, String constraintName,
      boolean missingOk) throws NoSuchObjectException;

  /**
   * Add a primary key to a table.
   * @param pks Columns in the primary key.
   * @return the name of the constraint, as a list of strings.
   * @throws InvalidObjectException The SQLPrimaryKeys list is malformed
   * @throws MetaException error accessing the RDMBS
   */
  List<SQLPrimaryKey> addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException;

  /**
   * Add a foreign key to a table.
   * @param fks foreign key specification
   * @return foreign key name.
   * @throws InvalidObjectException the specification is malformed.
   * @throws MetaException error accessing the RDBMS.
   */
  List<SQLForeignKey> addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException, MetaException;

  /**
   * Add unique constraints to a table.
   * @param uks unique constraints specification
   * @return unique constraint names.
   * @throws InvalidObjectException the specification is malformed.
   * @throws MetaException error accessing the RDBMS.
   */
  List<SQLUniqueConstraint> addUniqueConstraints(List<SQLUniqueConstraint> uks) throws InvalidObjectException, MetaException;

  /**
   * Add not null constraints to a table.
   * @param nns not null constraint specifications
   * @return constraint names.
   * @throws InvalidObjectException the specification is malformed.
   * @throws MetaException error accessing the RDBMS.
   */
  List<SQLNotNullConstraint> addNotNullConstraints(List<SQLNotNullConstraint> nns) throws InvalidObjectException, MetaException;

  /**
   * Add default values to a table definition.
   * @param dv list of default values
   * @return constraint names
   * @throws InvalidObjectException the specification is malformed.
   * @throws MetaException error accessing the RDBMS.
   */
  List<SQLDefaultConstraint> addDefaultConstraints(List<SQLDefaultConstraint> dv)
      throws InvalidObjectException, MetaException;

  /**
   * Add check constraints to a table.
   * @param cc check constraints to add
   * @return list of constraint names
   * @throws InvalidObjectException the specification is malformed
   * @throws MetaException error accessing the RDBMS
   */
  List<SQLCheckConstraint> addCheckConstraints(List<SQLCheckConstraint> cc) throws InvalidObjectException, MetaException;

}
