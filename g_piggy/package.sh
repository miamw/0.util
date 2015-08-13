#!/bin/bash

awk 'BEGIN{
	icu=5946.75*6.237
	tax_base=5793*12*3
	
	package=180846.67
	salory=15235

	bonus=15000
	icu=30000
	base=23545
	leave=base*1.5

	print package+salory+bonus+icu+leave
}'
