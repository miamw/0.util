%default Output	'/tmp/wm/ti/140516_analuisa/clk/$dat'
%default FilterDim	'publisher_managing_acct_id'
%default FilterList	'10000079672,10000101228,10000454789,10000650305'
%default YinstRoot     '/homes/mengwang/yinst_root/rmx_grid/lib/jars'
%default DataPath	'/data/rmx/prod'
%default Pre1hClk       'rmx_click_rollup'
%default Post5mImp        'post_tp_rmx_serve'
%default Post5mClk       'post_tp_rmx_click'
%default Post1hImp        'post_tp_rmx_serve'
%default Post1hJClk       'post_tp_rmx_joined_click'

REGISTER $YinstRoot/YahooZip.jar;
REGISTER $YinstRoot/explodehash_util.jar;
REGISTER /homes/mengwang/ana/util/mi_udf-*.jar;
REGISTER /homes/feipeng/install/udf/lib/jars/extractcookietime.jar;
DEFINE ExplodeHashList com.yahoo.util.explodehash.ExplodeHashList();
DEFINE FilterRecord com.yahoo.tp.mi_udf.FilterRecord('$FilterList');
DEFINE FilterRecordMatch com.yahoo.tp.mi_udf.FilterRecordMatch('$FilterList');
DEFINE ExtractCookieTime extractcookietime.BXDate();

-- Clean up
rmf -r $Output;

-- Load
POST_1H_J_CLK_RAW = LOAD '$DataPath/{$Post1hJClk}*_rollup/hourly/data/$dat*/' USING com.yahoo.yzip.hadoop.pig.YZipPigSchemaStorage('$DataPath/{$Post1hJClk}_rollup/hourly/schema/{$dat}00/{$Post1hJClk}_rollup.schema'); 

-- Extract Fields Needed
POST_1H_J_CLK_TTL1 = FOREACH POST_1H_J_CLK_RAW GENERATE
	event_guid,
	receive_time,
	tpcsnrt_valid AS valid,
	tpcsnrt_filters AS filters,
	publisher_managing_acct_id,
	publisher_acct_id,
	rm_section_id,
	advertiser_managing_acct_id,
	advertiser_acct_id,
	crtv_id,
	user_identifier,
	user_ip_address,
	user_agent,
	rm_country_woeid,
	rm_region_id,
	rm_creative_frequency,
	rm_creative_recency,
	join_receive_time,
	page_url,
	page_referrer,
	rm_http_referrer_url,
	rm_click_url,
	FLATTEN(ExplodeHashList(businessplatform, '\u0002', '\u0004', '\u0003')) AS hop:map[];

-- Advertiser Hop
POST_1H_J_CLK_TTL2 = FILTER POST_1H_J_CLK_TTL1 BY ((int)hop#'tcx' == 257 or (int)hop#'tcx' == 258);
POST_1H_J_CLK_TTL3 = FOREACH POST_1H_J_CLK_TTL2 GENERATE
	event_guid,
	receive_time,
	valid,
	filters,
	((hop#'amt' is not null) ? (double)hop#'amt' : 0) AS rev,
	publisher_managing_acct_id,
	publisher_acct_id,
	rm_section_id,
	advertiser_managing_acct_id,
	advertiser_acct_id,
	hop#'rmli' AS a_rmli,
	crtv_id,
	user_identifier,
	user_ip_address,
	user_agent,
	rm_country_woeid,
	rm_region_id,
	rm_creative_frequency,
	rm_creative_recency,
	join_receive_time,
	page_url,
	page_referrer,
	rm_http_referrer_url,
	rm_click_url;

-- Publisher Hop
POST_1H_J_CLK_TTL4 = FILTER POST_1H_J_CLK_TTL1 BY ((int)hop#'tcx' == 257 or (int)hop#'tcx' == 513);
POST_1H_J_CLK_TTL5 = FOREACH POST_1H_J_CLK_TTL4 GENERATE
	event_guid,
	receive_time,
	hop#'rmli' AS p_rmli;

POST_1H_J_CLK_TTL6 = JOIN POST_1H_J_CLK_TTL3 BY (event_guid,receive_time), POST_1H_J_CLK_TTL5 BY (event_guid,receive_time);
EVENT_TTL = FOREACH POST_1H_J_CLK_TTL6 GENERATE
	(POST_1H_J_CLK_TTL3::event_guid IS NULL?'-1':POST_1H_J_CLK_TTL3::event_guid) AS event_guid,
	(POST_1H_J_CLK_TTL3::receive_time IS NULL?'-1':POST_1H_J_CLK_TTL3::receive_time) AS receive_time,
	(valid IS NULL?1:valid) AS valid,
	(filters IS NULL?'-1':filters) AS filters,
	(rev IS NULL?0:rev) AS rev,
	(publisher_managing_acct_id IS NULL?'-1':publisher_managing_acct_id) AS publisher_managing_acct_id,
	(publisher_acct_id IS NULL?'-1':publisher_acct_id) AS publisher_acct_id,
	(rm_section_id IS NULL?-1:(long)rm_section_id) AS rm_section_id,
	(advertiser_managing_acct_id IS NULL?-1:(long)advertiser_managing_acct_id) AS advertiser_managing_acct_id,
	(advertiser_acct_id IS NULL?-1:(long)advertiser_acct_id) AS advertiser_acct_id,
	(a_rmli IS NULL?-1:a_rmli) AS a_rmli,
	(p_rmli IS NULL?-1:p_rmli) AS p_rmli,
	(crtv_id IS NULL?-1:(long)crtv_id) AS crtv_id,
	(user_identifier IS NULL?'-1':user_identifier) AS user_identifier,
	(user_ip_address IS NULL?'-1':user_ip_address) AS user_ip_address,
	(user_agent IS NULL?'-1':user_agent) AS user_agent,
	(rm_country_woeid IS NULL?-1:rm_country_woeid) AS rm_country_woeid,
	(rm_region_id IS NULL?-1:rm_region_id) AS rm_region_id,
	rm_creative_frequency AS rm_creative_frequency,
	rm_creative_recency AS rm_creative_recency,
	(join_receive_time IS NULL?'-1':join_receive_time) AS join_receive_time,
        (page_url IS NULL?'-1':page_url) AS page_url,
        (page_referrer IS NULL?'-1':page_referrer) AS page_referrer,
	(rm_http_referrer_url IS NULL?'-1':rm_http_referrer_url) AS rm_http_referrer_url,
	(rm_click_url IS NULL?'-1':rm_click_url) AS rm_click_url;
DESCRIBE EVENT_TTL;

-- Filter
EVENT_FLT = FILTER EVENT_TTL BY FilterRecord((chararray)$FilterDim);

--DESCRIBE EVENT_FLT;
--STORE EVENT_FLT INTO '$Output/data_filtered' USING PigStorage('\t');

-- Discard Rate
FILTER_DIST = GROUP EVENT_FLT BY $FilterDim;
FILTER_DIST = FOREACH FILTER_DIST GENERATE
	group AS $FilterDim,
	COUNT(EVENT_FLT) AS cnt_ttl,
	SUM(EVENT_FLT.valid) AS cnt_valid,
	((COUNT(EVENT_FLT)-SUM(EVENT_FLT.valid))*1.0/COUNT(EVENT_FLT)) AS discard_rete;

STORE FILTER_DIST INTO '$Output' USING PigStorage('\t');
