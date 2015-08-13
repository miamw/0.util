set mapred.job.queue.name=${queue};
set hive.map.aggr=true;

use ${db};

INSERT OVERWRITE TABLE 0_trim_benz
	PARTITION(dt='${dt}', hour)
SELECT
	bcookie AS bid,
	sid,
	is_page_view,
	event,
	event_trigger,
	event_tag,
	event_is_user_triggered,
	is_logged_in,
	yuid_age,
	yuid_timestamp,
	spaceid,
	ptyid,
	pty_name,
	bcookie_age,
	filter_tag,
	logged_event_timestamp,
	actual_event_timestamp,
	ip_address,
	user_agent,
	os_name,
	browser_name,
	page_domain,
	page_uri,
	page_search_term,
	referrer_domain,
	referrer_uri,
	referrer_search_term,
	mobile_app_name,
	pty_country,
	ydod,
	event_family,
	pty_family,
	pty_device,
	pty_experience,
	network,
	hour(from_unixtime(floor(logged_event_timestamp/1000))) AS hour
from benzene.daily_data
where dt='${dt}';
