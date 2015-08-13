ALLIP = load '$input' as (ip: chararray,trf: long,new_trf_rate: double,new_u_rate: double,login_trf_rate: double,uid_cnt: long,avg_uid_cnt: double,ua_cnt: long,avg_ua_cnt: double,spaceid_cnt: long,avg_spaceid_cnt: double,active_hour: long,peek_hour: long,active_5m: long,peek_5m: long, tag_sus: int, tag_pos: int);

ALLIP = filter ALLIP by (trf>$trf_th);

D = foreach ALLIP generate
	LOG(trf+1.0),
	LOG(1+200.0*new_trf_rate),
	LOG(1+200.0*new_u_rate),
	LOG(1+200.0*login_trf_rate),
	EXP(1.0/(1.0+EXP(uid_cnt*(-0.02)))),
	LOG(1+200*1.0/avg_uid_cnt),
	EXP(1.0/(1.0+EXP(ua_cnt*(-0.04)))),
	LOG(1+200*1.0/avg_ua_cnt),
	LOG(spaceid_cnt+1.0),
	LOG(1+200*1.0/avg_spaceid_cnt),
	active_hour,
	LOG(peek_hour+1.0),
	active_5m,
	LOG(peek_5m+1.0),
	((uid_cnt>=100 AND avg_ua_cnt>=19) OR (avg_ua_cnt>=19 AND active_hour>=12)?1:0) AS label,
	ip;

rmf $output;
store D into '$output';
