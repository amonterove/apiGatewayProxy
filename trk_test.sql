set colsep ,
set pagesize 0
set trimspool on
set headsep off
set linesize 1000

spool /home/amontero/PredictionTEST/csv/trk_test.csv

select /*+ RESULT_CACHE */
    *
from dual;

spool off
