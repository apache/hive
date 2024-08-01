COALESCE('First non-null', 1);
COALESCE(NULL, 'First non-null');
COALESCE(NULL, 'First non-null', 1);
COALESCE(NULL, NULL, 'First non-null', 1);