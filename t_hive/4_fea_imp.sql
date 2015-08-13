set mapred.job.queue.name=${queue};
set hive.map.aggr=true;

use mdl_yam_video;

DROP TABLE 4_fea_imp;
CREATE TABLE 4_fea_imp
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
	LOCATION '/tmp/wm/mdl_yam_video/4_fea_imp'
AS SELECT
	user_id,
	COUNT(*) AS imp,
	COUNT(DISTINCT sid) AS cnt_sid,
	COUNT(DISTINCT ip_address) AS cnt_ip,
	COUNT(DISTINCT user_agent) AS cnt_ua,
	COUNT(DISTINCT page_tld) AS cnt_tld,
	COUNT(DISTINCT app_name) AS cnt_app,
	COUNT(DISTINCT ts_1h) AS cnt_1h,
	COUNT(DISTINCT ts_5m) AS cnt_5m,
	COUNT(DISTINCT bid) AS cnt_bid
FROM (
	SELECT
		*,
		cast(timestamp/3600000 AS int) AS ts_1h,
		cast(timestamp/600000 AS int) AS ts_5m
	FROM 0_trim_yamimp
) A
GROUP BY user_id;	
