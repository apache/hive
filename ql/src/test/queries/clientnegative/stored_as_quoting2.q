--- Verifying that the proper identifier makes it to processStorageFormat and
--- displayed in the SemanticException.
--- This should be `ORC` in the SemanticException
create temporary table quoted_orc(i int) stored as ```ORC```;
