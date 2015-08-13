set mapred.job.queue.name=${queue};
set hive.map.aggr=true;

use ${db};

INSERT OVERWRITE TABLE 0_trim_engg
	PARTITION(dt='${cas}')
SELECT
	user_id,
	if(y_user_id is not null,y_user_id,bidding_time_b_id) AS bid,
	if(s_id is not null,s_id,bidding_time_s_id) AS sid,
	ad_call_id,
	engagement_type_id,
	engagement_time,
	ad_call_time,
	ip_address,
	page_tld,
	subdomain,
	app_name,
	user_agent,
	age,
	ad_unit_id,
	ad_id,
	creative_id
from kive.engagement_hourly
where dt='20150111' and user_id IN (${id});
