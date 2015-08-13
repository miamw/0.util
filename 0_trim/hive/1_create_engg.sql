use ${db};

DROP TABLE 0_trim_engg;
CREATE TABLE 0_trim_engg (
	user_id bigint,
	bid string,
	sid string,
	ad_call_id string,
	engagement_type_id string,
	engagement_time bigint,
	ad_call_time bigint,
	ip_address string,
	page_tld string,
	subdomain string,
	app_name string,
	user_agent string,
	age int,
	ad_unit_id string,
	ad_id string,
	creative_id string
)PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION '/tmp/wm/${db}/0_trim_engg';
