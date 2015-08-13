set default_parallel $para;

%default data_dir 'hdfs://dilithiumred-nn1.red.ygrid.yahoo.com:8020/projects/jaguar/prod/feeds/post_tp_annotated_gd2_impression/5m'

%default utilDir	'/homes/mengwang/root_git/util'
%default jarDir	'/homes/mengwang/util/jar'

IMPORT '$utilDir/util/util.pig';
REGISTER $jarDir/mi_udf-*.jar;
REGISTER $jarDir/YahooZip.jar;
REGISTER $jarDir/extractcookietime.jar;

rmf -r $out_path;

-- Main

TRIM = LOAD '$data_dir/data/$dat$hr*' USING com.yahoo.yzip.hadoop.pig.YZipPigSchemaStorage('$data_dir/schema/$ts_schema/post_tp_annotated_gd2_impression.schema');
TRIM = FOREACH TRIM GENERATE
	(event_guid IS NULL?'-1':event_guid) AS event_guid,
	(join_event_guid IS NULL?'-1':join_event_guid) AS join_event_guid,
	(ad_impression_id IS NULL?'-1':ad_impression_id) AS ad_impression_id,
	(join_ad_impression_id IS NULL?'-1':join_ad_impression_id) AS join_ad_impression_id,
	(publisher_sub_domain IS NULL?'-1':publisher_sub_domain) AS publisher_sub_domain,
	(gd_ad_position IS NULL?'-1':gd_ad_position) AS gd_ad_position,
	(gd_placement_id IS NULL?'-1':gd_placement_id) AS gd_placement_id,
	(acct_placement_id IS NULL?'-1':acct_placement_id) AS acct_placement_id,
	(cmpgn_placement_id IS NULL?'-1':cmpgn_placement_id) AS cmpgn_placement_id,
	(crtv_placement_id IS NULL?'-1':crtv_placement_id) AS crtv_placement_id,
	(booking_id IS NULL?'-1':booking_id) AS booking_id,
	(geo_best IS NULL?'-1':geo_best) AS geo_best,
	(receive_time IS NULL?'-1': receive_time) AS receive_time,
	(join_receive_time IS NULL?'-1': join_receive_time) AS join_receive_time,
	(tpssnrt_valid IS NULL?1:tpssnrt_valid) AS valid,
	(errors IS NULL?'-1':errors) AS errors,
	(publisher_managing_acct_id IS NULL?'-1':publisher_managing_acct_id) AS publisher_managing_acct_id,
	(publisher_acct_id IS NULL?'-1':publisher_acct_id) AS publisher_acct_id,
	(publisher_domain IS NULL?'-1':publisher_domain) AS publisher_domain,
	(page_url IS NULL?'-1':page_url) AS page_url,
	(site_id IS NULL?'-1':site_id) AS site_id,
	(gd_property IS NULL?'-1':gd_property) AS gd_property,
	(gd_space_id IS NULL?'-1':gd_space_id) AS gd_space_id,
	(section_id IS NULL?'-1':section_id) AS section_id,
	(publisher_country_code IS NULL?'-1':publisher_country_code) AS publisher_country_code,
	(advertiser_managing_acct_id IS NULL?'-1':advertiser_managing_acct_id) AS advertiser_managing_acct_id,
	(advertiser_acct_id IS NULL?'-1':advertiser_acct_id) AS advertiser_acct_id,
	(order_id IS NULL?'-1':order_id) AS order_id,
	(ad_grp_id IS NULL?'-1':ad_grp_id) AS ad_grp_id,
	(ad_grp_placement_id IS NULL?'-1':ad_grp_placement_id) AS ad_grp_placement_id,
	(cmpgn_id IS NULL?'-1':cmpgn_id) AS cmpgn_id,
	(crtv_id IS NULL?'-1':crtv_id) AS crtv_id,
	(advertiser_cost IS NULL?'0':advertiser_cost) AS advertiser_cost,
	(advertiser_cost_currency_id IS NULL?-1:advertiser_cost_currency_id) AS advertiser_cost_currency_id,
	(user_agent IS NULL?'-1':user_agent) AS user_agent,
	(user_ip_address IS NULL?'-1':user_ip_address) AS user_ip_address,
	(xcookie IS NULL?'-1':xcookie) AS xcookie,
	FLATTEN(STRSPLIT(xcookie, '&', 2)) as (uid:chararray, xcookie_suffix:chararray);

TRIM = FOREACH TRIM {
	cookie_age = ((receive_time IS NULL OR uid IS NULL)?-1:((long)receive_time/1000-(long)extractcookietime.BXDate(uid)));

	GENERATE
		*,
		cookie_age,
		(cookie_age/300==0?1:0) AS new_cookie;
}

-- Control name space
TRIM=ctrlNamespace(R, actual_event_timestamp);

DESCRIBE TRIM;
STORE TRIM INTO '$out_path';
fs -chmod 777 $out_path/..;fs -chmod -R 777 $out_path;
