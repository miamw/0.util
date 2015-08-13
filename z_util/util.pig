-- Group All
DEFINE ctrlOutNum1(IN) RETURNS OUT {
	$OUT = GROUP $IN ALL;
	$OUT = FOREACH $OUT GENERATE FLATTEN($IN);
};

-- Control name space
DEFINE ctrlOutNumN(IN, fld, num) RETURNS OUT {
	$OUT = GROUP $IN BY $fld PARALLEL $num;
	$OUT = FOREACH $OUT GENERATE FLATTEN($IN);
:xa
};

-- Get Count
DEFINE getCount(IN) RETURNS OUT {
        $OUT = GROUP $IN ALL;
        $OUT = FOREACH $OUT GENERATE
        	COUNT($IN) AS cnt;    	
};

-- Get Group Count
DEFINE getGrpCnt(IN, groupKey) RETURNS OUT {
	$OUT = GROUP $IN BY $groupKey;
	$OUT = FOREACH $OUT GENERATE
		group AS $groupKey,
		COUNT($IN) AS cnt;
};

-- Get Count, Trf
DEFINE getCountTrf(IN, trf) RETURNS OUT {
        $OUT = GROUP $IN ALL;
        $OUT = FOREACH $OUT GENERATE
        	COUNT($IN) AS cnt,
		SUM($IN.$trf) AS trf;
};

-- Get Group Count, Trf
DEFINE getGrpCntTrf(IN, groupKey, trf) RETURNS OUT {
	$OUT = GROUP $IN BY $groupKey;
	$OUT = FOREACH $OUT GENERATE
		group AS key,
		COUNT($IN) AS cnt,
		SUM($IN.$trf) AS trf;
};

-- Distribution%
DEFINE getDistSimuRate(IN,feature,trf) RETURNS DIST {
	DATA = FOREACH $IN GENERATE
		$feature AS feature,
		$trf AS trf;
        DIST1 = GROUP DATA BY feature;
        DIST2 = FOREACH DIST1 GENERATE
        	group AS feature,
        	COUNT(DATA) AS entity_cnt,
		SUM(DATA.trf) AS trf;

	OVERALL1 = GROUP DIST2 ALL;
	OVERALL = FOREACH OVERALL1 GENERATE
		SUM(DIST2.entity_cnt) AS entity_ttl,
		SUM(DIST2.trf) AS trf_ttl;

        DIST3 = CROSS DIST2,OVERALL;
        DIST4 = FOREACH DIST3 GENERATE
        	feature AS feature,
		entity_cnt AS entity_cnt,
		entity_ttl AS entity_ttl,
        	(float)entity_cnt/(float)entity_ttl AS entity_perc,
		trf AS trf,
		trf_ttl AS trf_ttl,
        	(float)trf/(float)trf_ttl AS trf_perc;
        $DIST = ORDER DIST4 BY feature ASC;       
};

DEFINE getDist(IN,feature) RETURNS DIST {
	DATA = FOREACH $IN GENERATE
		($feature IS NULL?-1:$feature) AS feature;
        DIST1 = GROUP DATA BY feature;
        DIST2 = FOREACH DIST1 GENERATE
        	group AS feature,
        	COUNT(DATA) AS entity_cnt;

	OVERALL1 = GROUP DATA ALL;
	OVERALL = FOREACH OVERALL1 GENERATE
		COUNT(DATA) AS entity_ttl;

        DIST3 = CROSS DIST2,OVERALL;
        DIST4 = FOREACH DIST3 GENERATE
        	feature AS feature,
		entity_cnt AS entity_cnt,
		entity_ttl AS entity_ttl,
        	(float)entity_cnt/(float)entity_ttl AS entity_perc;
        $DIST = ORDER DIST4 BY feature ASC;       
};
