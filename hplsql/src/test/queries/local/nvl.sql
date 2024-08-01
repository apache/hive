NVL('First non-null', 1);
NVL(NULL, 'First non-null');
NVL(NULL, 'First non-null', 1);
NVL(NULL, NULL, 'First non-null', 1);