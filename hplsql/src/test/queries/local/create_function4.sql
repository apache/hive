FUNCTION get(CODE VARCHAR2) RETURN VARCHAR2 AS
    TMPVAR VARCHAR2(10);
  BEGIN

    TMPVAR := '';
    
    IF (TMPVAR) = '' THEN
      RETURN '00080000';
    ELSE
      RETURN (TMPVAR);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN '00080000';
    WHEN OTHERS THEN
      RAISE;
  END;
  
get('abc');