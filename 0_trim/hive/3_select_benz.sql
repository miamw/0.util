use ${db};

select
	event_is_user_triggered,
	is_page_view,
	logged_event_timestamp,
	page_domain,
	mobile_app_name,
	ip_address,
	user_agent,
	yuid_age,
	bcookie_age,
	event_family,
	pty_family	
from 0_trim_benz
where sid='${id}' or bid='${id}'
order by logged_event_timestamp;
