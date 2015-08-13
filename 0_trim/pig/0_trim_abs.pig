set default_parallel $para;

--%default in_path	hdfs://uraniumblue-nn1.blue.ygrid.yahoo.com:8020/projects/FETL/LL_Web/$dat$hr*/yapache/{regular,late}/valid/part-m-*
%default in_path	hdfs://cobaltblue-nn1.blue.ygrid.yahoo.com:8020/projects/FETL/LL_Web/$dat$hr*/yapache/{regular,late}/valid/part-m-*
%default yinst_root	'/homes/mengwang/yinst_root'
%default utilDir	'/homes/mengwang/root_git/util'
%default jarDir	'/homes/mengwang/util/jar'

REGISTER $yinst_root/FETL_BASE_FEED/lib/jar/BaseFeed.jar;
REGISTER $jarDir/FETLProjector.jar;
REGISTER $jarDir/tpudf.jar;
REGISTER $jarDir/myudf_artifact-0.0.1-SNAPSHOT.jar;
REGISTER $jarDir/mi_udf*.jar;
DEFINE isVirtualSpaceid com.yahoo.tp.myudf.IsVirtualSpaceid();

rmf $out_path;

-------------------------- Main --------------------------

TRIM = LOAD '$in_path' USING com.yahoo.ccdi.fetl.base.pig.ABSLoader() AS (bcookie:chararray, timestamp:int,filterTag:int, dhrTag:int, transformEngineTag:int,simpleFields, mapFields, mapListFields);

TRIM = FOREACH TRIM {
	type = simpleFields#'type';
	cookiejar = mapFields#'cookiejar';
	device_info = mapFields#'device_info';
	pending_cookie = (cookiejar is not null and cookiejar#'~B' is not null and SIZE(cookiejar#'~B') > 2 ? (chararray)cookiejar#'~B' : '-1');
	bcookie_ts = simpleFields#'bcookie_ts';
	ip = simpleFields#'ip';
	user_agent = simpleFields#'user_agent';
	spaceid = simpleFields#'spaceid';
	woeid_country = simpleFields#'woeid_country';
	search_term = simpleFields#'search_term';
	yuid = simpleFields#'yuid';
	u_gender = simpleFields#'gender';
	u_year = simpleFields#'birth_year';
	regweek = simpleFields#'regweek';
	url = simpleFields#'url';
	os = simpleFields#'os';
	browser = simpleFields#'browser';
	osname = device_info#'osname';
	dtid = device_info#'dtid';
	model = device_info#'model';
	device_type = device_info#'device_type';
	screen_width = device_info#'screen_width';
	carrier = device_info#'carrier';
	osversion = device_info#'osversion';
	make = device_info#'make';
	bid = device_info#'bid';
	screen_height = device_info#'screen_height';
	esid = simpleFields#'esid';

GENERATE
	(bcookie IS NOT NULL and bcookie != ''? bcookie: (pending_cookie=='-1'? '-1' : SUBSTRING(pending_cookie,0,13)) ) AS bcookie,
	(filterTag IS NULL?0:filterTag) AS filterTag,
	(timestamp IS NULL?-1:timestamp) AS timestamp,
	(type IS NULL?'-1':type) AS type,
	(bcookie_ts IS NULL?'-1':bcookie_ts) AS bcookie_ts,
	(ip IS NULL?'-1':ip) AS ip,
	(user_agent IS NULL?'-1':user_agent) AS user_agent,
	(spaceid IS NULL?'-1':spaceid) AS spaceid,
	(woeid_country IS NULL?'-1':woeid_country) AS woeid_country,
	(search_term IS NULL?'-1':search_term) AS search_term,
	(yuid IS NULL?'-1':yuid) AS yuid,
	(u_gender IS NULL?'-1':u_gender) AS u_gender,
	(u_year IS NULL?'-1':u_year) AS u_year,
	(regweek IS NULL?'-1':regweek) AS regweek,
	(url IS NULL?'-1':url) AS url,
	(os IS NULL?'-1':os) AS os,
	(browser IS NULL?'-1':browser) AS browser,
	(device_type IS NULL?'-1':device_type) AS device_type,
	(osname IS NULL?'-1':osname) AS osname,
	(osversion IS NULL?'-1':osversion) AS osversion,
	(dtid IS NULL?'-1':dtid) AS dtid,
	(model IS NULL?'-1':model) AS model,
	(carrier IS NULL?'-1':carrier) AS carrier,
	(make IS NULL?'-1':make) AS make,
	(bid IS NULL?'-1':bid) AS bid,
	(screen_width IS NULL?'-1':screen_width) AS screen_width,
	(screen_height IS NULL?'-1':screen_height) AS screen_height,
	(esid is not null and esid != '' ? esid: '-1') as esid;
}

-- Control name space
TRIM=ctrlNamespace(TRIM,timestamp);

DESCRIBE TRIM;
STORE TRIM into '$out_path';

fs -chmod -R 777 $out_path; fs -chmod 777 $out_path/..
