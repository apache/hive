-- negative, references twice for result of funcion
explain select nkey, nkey + 1 from (select key + 1 as nkey, value from src) a;
