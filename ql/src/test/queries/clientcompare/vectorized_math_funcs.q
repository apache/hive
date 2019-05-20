--! qt:dataset:alltypesorc

select
   cdouble
  ,Round(cdouble, 2)
  ,Floor(cdouble)
  ,Ceil(cdouble)
  ,Rand(98007) as rnd
  ,Exp(ln(cdouble))
  ,Ln(cdouble)  
  ,Ln(cfloat)
  ,Log10(cdouble)
  -- Use log2 as a representative function to test all input types.
  ,Log2(cdouble)
  ,Log2(cfloat)
  ,Log2(cbigint)
  ,Log2(cint)
  ,Log2(csmallint)
  ,Log2(ctinyint)
  ,Log(2.0, cdouble)
  ,Pow(log2(cdouble), 2.0)  
  ,Power(log2(cdouble), 2.0)
  ,Sqrt(cdouble)
  ,Sqrt(cbigint)
  ,Bin(cbigint)
  ,Hex(cdouble)
  ,Conv(cbigint, 10, 16)
  ,Abs(cdouble)
  ,Abs(ctinyint)
  ,Pmod(cint, 3)
  ,Sin(cdouble)
  ,Asin(cdouble)
  ,Cos(cdouble)
  ,ACos(cdouble)
  ,Atan(cdouble)
  ,Degrees(cdouble)
  ,Radians(cdouble)
  ,Positive(cdouble)
  ,Positive(cbigint)
  ,Negative(cdouble)
  ,Sign(cdouble)
  ,Sign(cbigint)
from alltypesorc order by rnd limit 400;

