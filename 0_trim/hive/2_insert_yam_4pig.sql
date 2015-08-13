use mdl_yam_solu;

LOAD DATA INPATH '/tmp/wm/mdl_yam_solu/0_trim_pig_${act}/${date}/part*' OVERWRITE INTO TABLE 0_trim_pig_yam${act} PARTITION (dt='${date}');
