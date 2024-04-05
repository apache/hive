FUNCTION gettype(tag1 varchar2, srcvalue varchar2) return varchar2 as
    tmpVar varchar2(10);
  BEGIN

    if srcvalue is null or srcvalue = '@I' then
      return '@I';
    end if;

    if trim(tag1) = 'WHMM' then
      return '002';
    end if;

    if trim(tag1) = 'TCPJ' and srcvalue = '010105' then
      return '010105';
    end if;

    if trim(tag1) = 'TCPJ' and srcvalue != '010105' then
      return '003';
    end if;

    if trim(tag1) = 'TCPJ' and srcvalue != '010105' then
      return '003_ticket';
    end if;

    if trim(tag1) = 'TCJY' and srcvalue != '010105' then
      return '003_ticket';
    end if;

    if trim(tag1) = 'TCJY' and srcvalue != '010105' then
      return '003_ticket';
    end if;

    if trim(tag1) = 'YHHPD' then
      return '002_foreign';
    end if;

    if trim(tag1) = 'WHWZ' then
      return '002_foreign';
    end if;

    if trim(tag1) = 'WHLZ' then
      return '002_foreign';
    end if;

    if trim(tag1) = 'DEWZ' then
      return '024_out';
    end if;

    if trim(tag1) = 'DELZ' then
      return '024_out';
    end if;

    return srcvalue;

  END;

  gettype('YHHPD', 'a');
  gettype('YHHPD', '@I');