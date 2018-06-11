select assert_true(null is unknown);

select assert_true(true is not unknown);
select assert_true(false is not unknown);

select assert_true((null = null) is unknown);
select assert_true((null = false) is unknown);
select assert_true((null = true) is unknown);

select assert_true((select cast(null as boolean)) is unknown);
