create or replace package users as
  session_count int := 0;
  function get_count() return int; 
  procedure add(name varchar(100));
end;

create or replace package body users as
  function get_count() return int
  is
  begin
    return session_count;
  end; 
  procedure add(name varchar(100))
  is 
  begin
    session_count = session_count + 1;
  end;
end;

users.add('John');
users.add('Sarah');
users.add('Paul');
print 'Number of users: ' || users.get_count();