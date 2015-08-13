#!/bin/bash

function planHouse() {
	money_ttl=$1
	year_owe=$2
	rent=$3
	year_return=$4

	awk 'BEGIN{
		OFS="\t"
	
		v_ttl='$money_ttl'
		v_owe=120
		v_base=v_ttl-v_owe-25
		if(v_base<v_ttl*0.3-25) {
			v_base=v_ttl*0.3-25
			v_owe=v_ttl-v_base-25
		}
		start=v_base
		owe_ttl=v_owe
		r_o=0.04/12
		r_i=0.06/12
		f_in='$rent'
		f_add=0.417
		t_owe='$year_owe'
		t_ret='$year_return'
	
		###### Plan A
	
		## Monthly

		owe=v_owe
		v_owe_m=v_owe/(t_owe*12)
		earn=0
		for(i=1;i<=t_ret*12;i++) {
			ret=v_owe_m+owe*r_o	# need to return
			owe-=v_owe_m		# owe now
			earn=(earn+f_add+f_in-ret)*(1+r_i)
		}

		## Finally

		out_a = v_ttl-owe+earn

		###### Plan B

		out_b = v_base*((1+r_i)^(t_ret*12))

		print t_ret,out_a,out_b,(out_a-out_b),owe,earn,owe_ttl,start
	}'
}

function planHouseList() {
	money_ttl=$1
	rent=$2
	year_owe=$3

	if [[ $year_owe == "" ]]
	then
		year_owe=30
	fi

	echo "$money_ttl	$rent" > result_${money_ttl}_${year_owe}_${rent}
	cat head >> result_${money_ttl}_${year_owe}_${rent}
	for i in 30 20 10 5 3 1
	do
		planHouse $money_ttl $year_owe $rent $i >> result_${money_ttl}_${year_owe}_${rent}
	done
}

################################ Main

#planHouseList 200 0.35 20
#planHouseList 180 0.35 20
#planHouseList 150 0.3 20
planHouseList 120 0.3 30
