--- Verifying that the proper identifier makes it to processStorageFormat and
--- displayed in the SemanticException.
--- This should be DATE in the SemanticException
create temporary table quoted_date(i int) stored as `DATE`;
