REGISTER '$jar_dir/Base64.jar';
REGISTER '$jar_dir/stats_sketch.jar';
REGISTER '$jar_dir/myudf_artifact-0.0.1-SNAPSHOT.jar';
REGISTER '$jar_dir/mi_udf*.jar';

DEFINE dataToSketch com.yahoo.stats.pig.sketch.thetabb.DataToSketch ('$skt_th');
DEFINE estimate com.yahoo.stats.pig.sketch.thetabb.EstimateSketch();
DEFINE toBase64 com.yahoo.xuhy.base64.ByteArrayToBase64();
DEFINE toByte com.yahoo.xuhy.base64.Base64ToByteArray();
DEFINE merge_sketch com.yahoo.stats.pig.sketch.thetabb.MergeSketch ('$skt_th');
DEFINE getMeanSdCv com.yahoo.tp.mi_udf.GetMeanSdCv();

IMPORT '$util_dir/util.pig';

rmf -r $out_dir;

-------------------------- Func --------------------------
-------------------------- Main --------------------------

-- Load

FEA_H = LOAD '$in_dir' AS (dim: chararray,trf: double,active_10m: double,trf_new: double,trf_login: double,rate_new_trf: double,rate_login_trf: double,gap_cnt: double,gap_avg: double,gap_sd: double,gap_cv: double,cnt_ip: double,cnt_bcookie: double,cnt_sid: double,cnt_ua: double,cnt_os: double,cnt_browser: double,cnt_spaceid: double,cnt_uri: double,cnt_term: double,div_ip: double,div_bcookie: double,div_sid: double,div_ua: double,div_os: double,div_browser: double,div_spaceid: double,div_uri: double,div_term: double,skt_ip: bytearray,skt_bcookie: bytearray,skt_sid: bytearray,skt_ua: bytearray,skt_os: bytearray,skt_browser: bytearray,skt_spaceid: bytearray,skt_uri: bytearray,skt_term: bytearray);

-- Fea

OUT = feaAggSketch(FEA_H);
DESCRIBE OUT;
STORE OUT INTO '$out_dir/1_skt';

OUT = feaAggStat(FEA_H);
DESCRIBE OUT;
STORE OUT INTO '$out_dir/2_stat';

fs -chmod 777 $out_dir/..;fs -chmod -R 777 $out_dir;
