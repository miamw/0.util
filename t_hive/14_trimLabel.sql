SET hive.exec.dynamic.partition.mode=nonstrict;

use mdl_yam_video;

WITH YAM_TRIM AS (
	SELECT
		*
	FROM
		0_trim4Simu
	WHERE
		(dt='20150104' or dt='20150105') and (hour>=${hour}+2 and hour<=${hour}+25)
),LST AS (
	SELECT
		*
	FROM
		3_list
	WHERE
		dt='20150104' and hour=${hour}
)
FROM
	YAM_TRIM LEFT OUTER JOIN LST ON YAM_TRIM.ip_decimal=LST.ip_decimal
INSERT OVERWRITE TABLE 4_trimLabel
	PARTITION(action,dt)
SELECT
	YAM_TRIM.ip_decimal AS ip_decimal,
	YAM_TRIM.hour AS hour,
	if(LST.ip_decimal IS NOT NULL,1,0) AS tag,
	action,
	YAM_TRIM.dt AS dt;
