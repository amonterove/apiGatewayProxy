set colsep ,
set pagesize 0
set trimspool on
set headsep off
set linesize 1000

spool /root/PredictionTEST/csv/trk_test.csv

select /*+ RESULT_CACHE */
    distinct
    *
from dual;

spool off
