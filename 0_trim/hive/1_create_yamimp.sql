use ${db};

DROP TABLE 0_trim_yamimp;
CREATE TABLE 0_trim_yamimp (
	user_id bigint,
	bid string,
	sid string,
	ip_address int,
	user_agent string,
	account_id string,
	device_id string,
	ad_call_id bigint,
	ad_call_time bigint,
	market_id int,
	publisher_id bigint,
	layout_id int,
	creative_id bigint,
	page_tld string,
	app_name string,
	model_id int,
	country_id int,
	user_local_hour int,
	user_local_day_of_week int,
	age int,
	gender int,
	is_new_user int,
	user_id_type int,
	timestamp bigint,
	subdomain string,
	tp_score int,
	tp_model_id bigint,
	test_flag int,
	rmx_section_id bigint,
	s_id string,
	keyword_id bigint,
	bidding_time_b_cookie string,
	bidding_time_b_id string,
	bidding_time_s_id string,
	is_tp_valid int,
	tp_output struct<tp_valid:int,tp_score:int,tp_filters:string>
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION '/tmp/wm/${db}/0_trim_yamimp';
