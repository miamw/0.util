%default DataPath	'/data/rmx/prod'
%default Post1hImp        'post_tp_rmx_serve'
%default Post1hJClk       'post_tp_rmx_joined_click'
%default Pre1hCnv       'rmx_conv'
%default YinstRoot     '/homes/mengwang/yinst_root/rmx_grid/lib/jars'

REGISTER $YinstRoot/YahooZip.jar;
REGISTER $YinstRoot/explodehash_util.jar;
REGISTER /homes/mengwang/ana/util/mi_udf-*.jar;
REGISTER /homes/feipeng/install/udf/lib/jars/extractcookietime.jar;
DEFINE ExplodeHashList com.yahoo.util.explodehash.ExplodeHashList();

-- Clean up
rmf -r $out_path;

-- Impression
IMP = LOAD '$DataPath/{$Post1hImp}*_rollup/hourly/data/$dat$hr/' USING com.yahoo.yzip.hadoop.pig.YZipPigSchemaStorage('$DataPath/{$Post1hImp}_rollup/hourly/schema/$dat$hr/{$Post1hImp}_rollup.schema');
IMP = FOREACH IMP GENERATE
	tpssnrt_valid AS valid,
	rm_section_id,
	FLATTEN(ExplodeHashList(offers, '\u0002', '\u0004', '\u0003')) AS offer:map[];
IMP = FILTER IMP BY (offer#'rwmt'==1 OR offer#'rwmt'==2);
IMP = FOREACH IMP GENERATE
	valid,
	rm_section_id,
	offer#'amid' AS advertiser_managing_acct_id,
        offer#'crid' AS crtv_id,
	FLATTEN(ExplodeHashList(offer#'businessplatform', '\u0007', '\u0006', '\u0005')) AS hop:map[];
IMP = FILTER IMP BY ((int)hop#'tcx' == 257 or (int)hop#'tcx' == 258);
IMP = FOREACH IMP GENERATE
	'imp' AS type,
	'$dat' AS date,
	(valid IS NULL?1:valid) AS valid,
	(rm_section_id IS NULL?-1:rm_section_id) AS rm_section_id,
	(advertiser_managing_acct_id IS NULL?'-1':advertiser_managing_acct_id) AS advertiser_managing_acct_id,
	(hop#'rmli' IS NULL?'-1':hop#'rmli') AS a_rmli,
	(crtv_id IS NULL?'-1':crtv_id) AS crtv_id,
	((hop#'amt' IS NOT NULL) ? (double)hop#'amt' : 0) AS rev;

-- Click
CLK = LOAD '$DataPath/{$Post1hJClk}*_rollup/hourly/data/$dat$hr/' USING com.yahoo.yzip.hadoop.pig.YZipPigSchemaStorage('$DataPath/{$Post1hJClk}_rollup/hourly/schema/$dat$hr/{$Post1hJClk}_rollup.schema');
CLK = FILTER CLK BY (rm_winning_marketplace_type==1 OR rm_winning_marketplace_type==2);
CLK = FOREACH CLK GENERATE
	tpcsnrt_valid AS valid,
	rm_section_id,
	advertiser_managing_acct_id,
	crtv_id,
	FLATTEN(ExplodeHashList(businessplatform, '\u0002', '\u0004', '\u0003')) AS hop:map[];
CLK = FILTER CLK BY ((int)hop#'tcx' == 257 or (int)hop#'tcx' == 258);
CLK = FOREACH CLK GENERATE
	'clk' AS type,
	'$dat' AS date,
	(valid IS NULL?1:valid) AS valid,
	(rm_section_id IS NULL?-1:rm_section_id) AS rm_section_id,
	(advertiser_managing_acct_id IS NULL?'-1':advertiser_managing_acct_id) AS advertiser_managing_acct_id,
	(hop#'rmli' IS NULL?'-1':hop#'rmli') AS a_rmli,
	(crtv_id IS NULL?'-1':crtv_id) AS crtv_id,
	((hop#'amt' IS NOT NULL) ? (double)hop#'amt' : 0) AS rev;

-- Conversion
CNV = LOAD '$DataPath/{$Pre1hCnv}*_rollup/hourly/data/$dat$hr/' USING com.yahoo.yzip.hadoop.pig.YZipPigSchemaStorage('$DataPath/{$Pre1hCnv}_rollup/hourly/schema/$dat$hr/{$Pre1hCnv}_rollup.schema');
CNV = FILTER CNV BY (rm_winning_marketplace_type==1 OR rm_winning_marketplace_type==2);
CNV = FOREACH CNV GENERATE
	rm_section_id,
	advertiser_managing_acct_id,
	crtv_id,
	FLATTEN(ExplodeHashList(businessplatform, '\u0002', '\u0004', '\u0003')) AS hop:map[];
CNV = FILTER CNV BY ((int)hop#'tcx' == 257 or (int)hop#'tcx' == 258);
CNV = FOREACH CNV GENERATE
	'cnv' AS type,
	'$dat' AS date,
	1 AS valid,
	(rm_section_id IS NULL?-1:rm_section_id) AS rm_section_id,
	(advertiser_managing_acct_id IS NULL?'-1':advertiser_managing_acct_id) AS advertiser_managing_acct_id,
	(hop#'rmli' IS NULL?'-1':hop#'rmli') AS a_rmli,
	(crtv_id IS NULL?'-1':crtv_id) AS crtv_id,
	((hop#'amt' IS NOT NULL) ? (double)hop#'amt' : 0) AS rev;

-- Union

OUT = UNION IMP, CLK, CNV;

DESCRIBE OUT;
STORE OUT INTO '$out_path';
fs -chmod -R 777 $out_path;
