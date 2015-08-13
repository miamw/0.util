set default_parallel 100

register /homes/feipeng/install/lib/jars/qiuxe_udf-1.0-SNAPSHOT.jar
register /homes/feipeng/install/lib/jars/analytics_commons-1.0.0.jar
register /homes/feipeng/install/udf/lib/jars/extractcookietime.jar
register /home/gs/pig/current/lib//piggybank.jar;
register /home/gs/pig/current/lib/json-simple-1.1.jar;

DEFINE IPLong2String com.yahoo.qiuxe.pig.udf.IPLong2String();

%default imp_dir 'hdfs://dilithiumblue-nn1.blue.ygrid.yahoo.com:8020/projects/kite/prod/internal/core/impression/5m/$date*/p*'

rmf -r $output1_dir;

imp_events = LOAD '$imp_dir' USING org.apache.pig.piggybank.storage.avro.AvroStorage();

imp_events = foreach imp_events generate
	user_id,
    user_id_type,
    s_id,
	(int)ip_address,
	user_agent,
	ad_call_id,
	ad_call_time,
	publisher_id,
    ad_unit_id,
    account_id,
    layout_id,
	order_id,
	line_id,
	ad_id,
	creative_id,
	page_tld,
	app_name,
	country_id,
	region_id,
	city_id,
	postal_code_id,
	os_version_id,
	browser_type_id,
	os_id,
	timezone_offset,
    user_local_hour,
    user_local_day_of_week,
	is_new_user,
	ad_position_id,
    location_type_id,
	device_id,
	y_user_id,
	subdomain,
	bidding_time_b_cookie,
	bidding_time_b_id,
	bidding_time_s_id,
	is_tp_valid,
    serving_ip_address,
    advertiser_cost,
    test_flag,
	tp_output.tp_valid,
	tp_output.tp_score,
	tp_output.tp_filters;

imp_events = foreach imp_events generate
    user_id,
    user_id_type,
    s_id,
    ip_address,
    IPLong2String(ip_address) as ip_decimal,
    (y_user_id is null ? bidding_time_b_id : y_user_id) as bcookie,
    user_agent,
    ad_call_id,
    ad_call_time,
    publisher_id,
    ad_unit_id,
    account_id,
    layout_id,
    order_id,
    line_id,
    ad_id,
    creative_id,
    page_tld,
    app_name,
    country_id,
    region_id,
    city_id,
    postal_code_id,
    os_version_id,
    browser_type_id,
    os_id,
    timezone_offset,
    user_local_hour,
    user_local_day_of_week,
    is_new_user,
    ad_position_id,
    location_type_id,
    device_id,
    y_user_id,
    subdomain,
    bidding_time_b_cookie,
    bidding_time_b_id,
    bidding_time_s_id,
    is_tp_valid,
    serving_ip_address,
    advertiser_cost,
    test_flag,
    tp_valid,
    tp_score,
    tp_filters;  

imp_events = foreach imp_events generate
    user_id,
    user_id_type,
    s_id,
    ip_address,
    ip_decimal,
    bcookie,
    extractcookietime.BXDate(bcookie) as bcookie_time, 
    user_agent,
    ad_call_id,
    ad_call_time,
    publisher_id,
    ad_unit_id,
    account_id,
    layout_id,
    order_id,
    line_id,
    ad_id,
    creative_id,
    page_tld,
    app_name,
    country_id,
    region_id,
    city_id,
    postal_code_id,
    os_version_id,
    browser_type_id,
    os_id,
    timezone_offset,
    user_local_hour,
    user_local_day_of_week,
    is_new_user,
    ad_position_id,
    location_type_id,
    device_id,
    y_user_id,
    subdomain,
    bidding_time_b_cookie,
    bidding_time_b_id,
    bidding_time_s_id,
    is_tp_valid,
    serving_ip_address,
    advertiser_cost,
    test_flag,
    tp_valid,
    tp_score,
    tp_filters;

g_tmp = group imp_events by (ad_call_id, ad_call_time, bcookie, ip_address) parallel 100;

g_imp_events = foreach g_tmp generate
	FLATTEN(imp_events);

store g_imp_events into '$output1_dir' using PigStorage('\u0001');

fs -chmod -R 777 $output1_dir;
fs -chmod 777 $output1_dir/../;
