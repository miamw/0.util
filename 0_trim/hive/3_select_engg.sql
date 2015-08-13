use mdl_yam_video;

select
	ad_call_id,
	ad_call_time,
	engagement_type_id,
	engagement_time
from 0_trim_engg
where user_id=${id}
order by ad_call_time, engagement_time;
