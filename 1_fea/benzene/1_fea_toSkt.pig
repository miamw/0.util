REGISTER '$jar_dir/Base64.jar';
REGISTER '$jar_dir/stats_sketch.jar';
REGISTER '$jar_dir/mi_udf*.jar';

DEFINE dataToSketch com.yahoo.stats.pig.sketch.thetabb.DataToSketch ('$skt_th');
DEFINE estimate com.yahoo.stats.pig.sketch.thetabb.EstimateSketch();
DEFINE toBase64 com.yahoo.xuhy.base64.ByteArrayToBase64();
DEFINE toByte com.yahoo.xuhy.base64.Base64ToByteArray();
DEFINE merge_sketch com.yahoo.stats.pig.sketch.thetabb.MergeSketch ('$skt_th');
DEFINE getTimeGap com.yahoo.tp.mi_udf.GetTimeGap('$session_time');

IMPORT '$util_dir/util.pig';

rmf -r $out_dir;

-------------------------- Func --------------------------
-------------------------- Main --------------------------

-- Load

TRIM = LOAD '$in_dir/$dat$hr.bz2' USING PigStorage('\u0001') AS (is_page_view: boolean,event: chararray,event_trigger: chararray,event_tag: map[],event_is_user_triggered: boolean,is_logged_in: boolean,sid: chararray,yuid_age: chararray,yuid_timestamp: chararray,spaceid: chararray,ptyid: chararray,pty_name: chararray,pty_country: chararray,bcookie: chararray,bcookie_mobile_source: chararray,bcookie_version: chararray,bcookie_timestamp: chararray,bcookie_age: long,filter_tag: map[],logged_event_timestamp: long,actual_event_timestamp: long,ip_address: chararray,ip_version: int,user_agent: chararray,os_name: chararray,os_version: chararray,browser_name: chararray,browser_version: chararray,page_domain: chararray,page_uri: chararray,page_search_term: chararray,referrer_domain: chararray,referrer_uri: chararray,referrer_search_term: chararray,server_name: chararray,mobile_device_make: chararray,mobile_device_model: chararray,mobile_device_version: chararray,mobile_device_locale: chararray,mobile_device_id: chararray,mobile_device_idfv: chararray,mobile_device_orientation: chararray,mobile_device_connectivity: chararray,mobile_device_timezone_offset: chararray,mobile_device_country: chararray,mobile_device_resolution: chararray,mobile_device_carrier: chararray,mobile_app_name: chararray,mobile_app_version: chararray,mobile_app_screen_name: chararray,demog_info: map[],country_id: bytearray,ydod: chararray,event_family: chararray,pty_family: chararray,pty_device: chararray,pty_experience: chararray,dt: chararray,network: chararray,ad_info: {innertuple: (innerfield: map[])});

-- pv, bcookie

TRIM_FLT = FILTER TRIM BY (is_page_view==true AND bcookie IS NOT NULL);
OUT = feaToSketch(TRIM_FLT, bcookie);
DESCRIBE OUT;
STORE OUT INTO '$out_dir/pv_bcookie';

-- pv, sid

TRIM_FLT = FILTER TRIM BY (is_page_view==true AND sid IS NOT NULL);
OUT = feaToSketch(TRIM_FLT, sid);
DESCRIBE OUT;
STORE OUT INTO '$out_dir/pv_sid';

fs -chmod 777 $out_dir/..;fs -chmod -R 777 $out_dir;
