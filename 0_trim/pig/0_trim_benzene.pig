set default_parallel $para;

rmf -r $out_path.bz2;

---- Load & Filter

---- hourly
--D0 = LOAD 'benzene.hourly_data' USING org.apache.hcatalog.pig.HCatLoader();
--D = filter D0 by dt=='$dat$hr';

---- daily
D0 = LOAD 'benzene.daily_data' USING org.apache.hcatalog.pig.HCatLoader();
D = filter D0 by dt=='$dat';

-- Fields

R = foreach D generate
	is_page_view,
	event,
	event_trigger,
	event_tag,
	event_is_user_triggered,
	is_logged_in,
	sid,
	yuid_age,
	yuid_timestamp,
	spaceid,
	ptyid,
	pty_name,
	pty_country,
	bcookie,
	bcookie_mobile_source,
	bcookie_version,
	bcookie_timestamp,
	bcookie_age,
	filter_tag,
	logged_event_timestamp,
	actual_event_timestamp,
	ip_address,
	ip_version,
	user_agent,
	os_name,
	os_version,
	browser_name,
	browser_version,
	page_domain,
	page_uri,
	page_search_term,
	referrer_domain,
	referrer_uri,
	referrer_search_term,
	server_name,
	mobile_device_make,
	mobile_device_model,
	mobile_device_version,
	mobile_device_locale,
	mobile_device_id,
	mobile_device_idfv,
	mobile_device_orientation,
	mobile_device_connectivity,
	mobile_device_timezone_offset,
	mobile_device_country,
	mobile_device_resolution,
	mobile_device_carrier,
	mobile_app_name,
	mobile_app_version,
	mobile_app_screen_name,
	demog_info,
	ip_geo_info#'country_id' AS country_id,
	ydod,
	event_family,
	pty_family,
	pty_device,
	pty_experience,
	dt,
	network,
	ad_info;

-- Control name space
TRIM=ctrlNamespace(R, actual_event_timestamp);

describe TRIM;
store TRIM into '$out_path.bz2' using PigStorage('\u0001');
fs -chmod -R 777 $out_path.bz2; fs -chmod 777 $out_path.bz2/../;
