create table cx1(bool0 boolean);

select cast(NULL as boolean) or bool0 from cx1;

select NULL or bool0 from cx1;

select cast(NULL as boolean) and bool0 from cx1;

select NULL and bool0 from cx1;
