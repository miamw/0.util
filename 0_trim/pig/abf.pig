%default output_dir /tmp/wm/ti_7005785/0_trim/$dat$hr/
%default input_abf /data/FETL/ABF/$dat$hr*/2*/PAGE/
register /homes/zqf/application/fetl_base/lib/jars/BaseFeed.jar;
register /homes/zqf/application/fetl_project/lib/jars/FETLProjector.jar;
register /homes/dayong/yinst_root/rmx_grid/lib/jars/tpudf.jar;
register /homes/mengwang/ana/util/mi_udf*.jar;
DEFINE GenerateSessionID com.yahoo.tp.pig.udf.GenerateSessionID();
IMPORT '/homes/mengwang/ana/util/util.pig';

rmf $output_dir;

----------------------- Func -----------------------

DEFINE initABF(IN_PATH) RETURNS OUT {
	-- Load
	RAW_EVENT = LOAD '$IN_PATH' USING com.yahoo.ccdi.fetl.sequence.pig.Projector('type, event_type, valid, filterTag, tp_tags, bcookie, ip, timestamp, user_agent, src_pty, spaceid, server_code, woeid_country,search_term,yuid,guid,bcookie_ts,cookie_id,demog.g,demog.y') AS (type, event_type, valid, filterTag, tp_tags, bcookie, ip, timestamp, user_agent, src_pty, spaceid, server_code, woeid_country,search_term,yuid,guid,bcookie_ts,cookie_id,gender,year);
	
	-- PV
	PV = FILTER RAW_EVENT BY (type=='p' and event_type=='page');
	$OUT = FOREACH PV {
		timestamp = (timestamp IS NULL?-1:timestamp);
		bcookie_ts = (bcookie_ts IS NULL?-1:bcookie_ts);
		ts_diff = ((timestamp==-1 OR bcookie_ts==-1)?-1:(timestamp-bcookie_ts));
		is_new = ((ts_diff<300 AND ts_diff>=0)?1:0);
		reg_user = ((gender is not null and year is not null) ? 1 : 0);
		tp_valid = (com.yahoo.ccdi.fetl.pig.udf.DecodeFilterTags((int)filterTag).tp_robot == 1 ? 0 : 1);
		tp_filters = (tp_tags IS NOT NULL AND tp_tags!='' ? (chararray)tp_tags : '');
	
		GENERATE
			(ip IS NULL?'-1':ip) AS ip,
			(bcookie IS NULL?'-1':bcookie) AS bcookie,
			timestamp AS timestamp,
			bcookie_ts AS bcookie_ts,
			ts_diff AS ts_diff,
			is_new AS is_new,
			(user_agent IS NULL?'-1':user_agent) AS user_agent,
			(src_pty IS NULL?'-1':src_pty) AS src_pty,
			(spaceid IS NULL?'-1':spaceid) AS spaceid,
			(server_code IS NULL?'-1':server_code) AS server_code,
			(woeid_country IS NULL?'-1':woeid_country) AS woeid_country,
			(valid IS NULL?'-1':valid) AS valid,
			(filterTag IS NULL?'-1':filterTag) AS filterTag,
			tp_valid AS tp_valid,
			tp_filters AS tp_filters,
			(gender IS NULL?'-1':gender) AS gender,
			(year IS NULL?'-1':year) AS year,
			reg_user AS reg_user,
			(yuid IS NULL?'-1':yuid) AS yuid,
			(guid IS NULL?'-1':guid) AS guid,
			(search_term IS NULL?'-1':search_term) AS search_term;
	}
};

----------------------- Main -----------------------

VALID = initABF('$input_abf/Valid/part*');
DESCRIBE VALID;
STORE VALID INTO '$output_dir/valid' Using PigStorage('\u0001');

INVALID = initABF('$input_abf/Invalid/part*');
DESCRIBE INVALID;
STORE INVALID INTO '$output_dir/invalid' Using PigStorage('\u0001');

fs -chmod -R 777 $output_dir;
