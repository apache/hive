FROM src key
INSERT OVERWRITE TABLE dest1 SELECT key.key WHERE key.value < 100
