use mdl_yam_video;

select
	ad_call_id,
	timestamp,
	page_tld,
	app_name,
	rmx_section_id,
	creative_id,
	layout_id,
	ip_address,
	user_agent,
	tp_output,
	sid,
	bid
from 0_trim_yamclk
where user_id=${id}
order by timestamp;
