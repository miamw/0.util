use ${db};

DROP TABLE 0_trim_benz;
CREATE TABLE 0_trim_benz (
	bid string,
	sid string,
	is_page_view boolean,
	event string,
	event_trigger string,
	event_tag string,
	event_is_user_triggered boolean,
	is_logged_in boolean,
	yuid_age string,
	yuid_timestamp string,
	spaceid string,
	ptyid string,
	pty_name string,
	bcookie_age string,
	filter_tag string,
	logged_event_timestamp bigint,
	actual_event_timestamp bigint,
	ip_address string,
	user_agent string,
	os_name string,
	browser_name string,
	page_domain string,
	page_uri string,
	page_search_term string,
	referrer_domain string,
	referrer_uri string,
	referrer_search_term string,
	mobile_app_name string,
	pty_country string,
	ydod string,
	event_family string,
	pty_family string,
	pty_device string,
	pty_experience string,
	network string
)
PARTITIONED BY (dt string, hour int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION '/tmp/wm/${db}/0_trim_benz';
