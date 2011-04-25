select ewah_bitmap_and(array(13,2,4,8589934592,4096,0), array(13,2,4,8589934592,4096,0)) from src limit 1;

select ewah_bitmap_and(array(13,2,4,8589934592,4096,0), array(8,2,4,8589934592,128,0)) from src limit 1;
