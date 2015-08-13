events = load '$input_dir' as (score:double, label, ip);

events = foreach events generate
	(score/$c) as score,
	label as label,
	ip as ip;

events_grp = group events all;
stats = foreach events_grp generate MAX(events.score) as max_score, MIN(events.score) as min_score;

events = foreach events generate 
	ip,
	label,
	(score-(double)stats.min_score)/((double)stats.max_score-(double)stats.min_score) as score;
--events = filter events by (score>0.7);

rmf $output_dir;
store events into '$output_dir';
