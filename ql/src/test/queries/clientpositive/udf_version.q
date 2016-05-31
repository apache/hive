-- Normalize the version info
SELECT regexp_replace(version(), '.+ r\\w+', 'VERSION rGITHASH');
