SET hive.exec.dynamic.partition.mode=nonstrict;
--SET hive.exec.max.dynamic.partitions=1000;

use mdl_yam_video;

WITH TRIM_IMP AS (
	SELECT 
		ip_decimal,
		floor(ad_call_time / 3600000) AS hour,
		dt
	FROM
		feipengdb.yam_${action}_trim
	WHERE
		dt REGEXP '2015010[3-9]'
)
FROM TRIM_IMP
INSERT OVERWRITE TABLE 0_trim4Simu
	PARTITION(action='${action}',dt)
SELECT
	ip_decimal,
	hour,
	dt;
