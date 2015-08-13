set mapred.job.queue.name=${queue};
use mdl_yam_video;

INSERT OVERWRITE TABLE 3_list
	PARTITION(dt='${dt}')
SELECT
	dt,
	ip_decimal,
	hour
FROM 2_fea_hr
WHERE
	dt='${dt}' AND
	((cnt_clk>=3 and cnt_clk<8 and ratio_new_bcookie_clk==1.0 and ctr>=0.05) or (cnt_clk>=8 and ratio_new_bcookie_clk==1 and ctr>=0.01));
