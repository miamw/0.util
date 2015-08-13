set mapred.job.queue.name=${queue};
set hive.map.aggr=true;

use ${db};

INSERT OVERWRITE TABLE 0_trim_yamclk
	PARTITION(dt='${cas}')
SELECT
	user_id,
	if(y_user_id is not null,y_user_id,bidding_time_b_id) AS bid,
	if(s_id is not null,s_id,bidding_time_s_id) AS sid,
	ip_address,
	user_agent,
	account_id,
	device_id,
	ad_call_id,
	ad_call_time,
	market_id,
	publisher_id,
	layout_id,
	creative_id,
	page_tld,
	app_name,
	model_id,
	country_id,
	user_local_hour,
	user_local_day_of_week,
	age,
	gender,
	is_new_user,
	user_id_type,
	timestamp,
	subdomain,
	tp_score,
	tp_model_id,
	test_flag,
	rmx_section_id,
	s_id,
	keyword_id,
	bidding_time_b_cookie,
	bidding_time_b_id,
	bidding_time_s_id,
	is_tp_valid,
	click_tp_output
from kive.click_hourly
where dt='20150111' and user_id IN (${id});
