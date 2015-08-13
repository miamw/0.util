set mapred.job.queue.name=${queue};
set hive.map.aggr=true;

use mdl_yam_video;

-- Get ctr

--DROP TABLE z_trim_imp${dt};
--CREATE TABLE z_trim_imp${dt}
--AS
--SELECT
--	ip_decimal,
--	hr,
--	COUNT(*) AS cnt_imp
--FROM
--	(
--		SELECT
--			user_id AS ip_decimal,
--			CAST(ad_call_time/3600000 AS int) AS hr
--		FROM feipengdb.yam_imp_trim
--		WHERE
--			dt='${dt}'
--	) TRIM_IMP
--GROUP BY ip_decimal,hr;
--
--DROP TABLE z_trim_clk${dt};
--CREATE TABLE z_trim_clk${dt}
--AS SELECT
--	ip_decimal,
--	hr,
--	COUNT(*) AS cnt_clk,
--	SUM(new_user)/COUNT(*) AS ratio_new_bcookie_clk
--FROM
--	(
--		SELECT
--			user_id AS ip_decimal,
--			CAST(ad_call_time/3600000 AS int) AS hr,
--			if((floor(ad_call_time / 1000) - bcookie_time) <= 3600, 1, 0) AS new_user
--		FROM feipengdb.yam_clk_trim
--		WHERE
--			dt='${dt}'
--	) TRIM_CLK
--GROUP BY ip_decimal,hr;

INSERT OVERWRITE TABLE 2_fea_hr
	PARTITION(dt='${dt}')
SELECT
	z_trim_imp1.ip_decimal,
	z_trim_imp1.hr,
	ratio_new_bcookie_clk,
	if(cnt_imp IS NULL,0,cnt_imp) AS cnt_imp,
	if(cnt_clk IS NULL,0,cnt_clk) AS cnt_clk,
	if((cnt_clk=0 OR cnt_clk IS NULL),0,if((cnt_imp=0 OR cnt_imp IS NULL),9999,cnt_clk*1.0/cnt_imp)) AS ctr
FROM z_trim_imp1 FULL OUTER JOIN z_trim_clk1
ON
	z_trim_imp1.ip_decimal = z_trim_clk1.ip_decimal AND
	z_trim_imp1.hr = z_trim_clk1.hr
ORDER BY
	cnt_imp DESC;

DROP TABLE z_trim_imp${dt};
DROP TABLE z_trim_clk${dt};
