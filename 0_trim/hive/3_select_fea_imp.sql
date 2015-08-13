set mapred.job.queue.name=${queue};
set hive.map.aggr=true;

use mdl_yam_video;

select
	*
from 4_fea_imp
where user_id=${id};
