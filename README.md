Tools to create OBSMON SQLite files from ODB2 and pysurfex output

Example:
```
odb2sqlite \
 --run-settings /nobackup/forsk/sm_tryas/git/obsmon-tools/obsmontools/data/run_settings.json \
 --obsmon-config /nobackup/forsk/sm_tryas/git/obsmon-tools/obsmontools/data/obsmon_config.json \
 --odb-config /nobackup/forsk/sm_tryas/git/obsmon-tools/obsmontools/data/odb_config.json \
 --datapath /nobackup/forsk/sm_tryas/harmonie/odb2_in_aa/archive/2025/11/05/12 \
 --suffix mfb \
 --dtg 2025110912 \
 --output ccma.db
 ```
