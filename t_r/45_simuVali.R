########################################## Func ##########################################

main = function() {

	# 1. Read data
	inData = read.table("data/1_feature",sep='\t')
		# Name columns
 	colName = c("s_bcookie","s_pv","s_pv_nn","s_query_cnt","s_query_uri_cnt_mean","s_query_uri_cnt_std","s_query_turning_cnt_mean","s_query_turning_cnt_std","s_query_pv_mean","s_query_pv_std","s_query_term_cnt_mean","s_query_term_cnt_std","e_uid","e_cnt_tot","e_cnt_1h","e_cnt_query","e_cnt_trigger","e_cnt_click","e_cnt_view","e_new_rate_1","e_new_rate_2","e_new_rate","e_login_rate_1","e_login_rate_2","e_login_rate","e_referrer_rate_1","e_referrer_rate_2","e_referrer_rate","e_query_rate_1","e_query_rate_2","e_query_rate","e_sugg_rate_1","e_sugg_rate_2","e_sugg_rate","e_ctr_1","e_ctr_2","e_ctr","e_ctr_next_page_1","e_ctr_next_page_2","e_ctr_next_page","e_ctr_search_result_1","e_ctr_search_result_2","e_ctr_search_result","d_uid","d_cnt","d_cnt_1h","d_cnt_5m","d_is_pv","d_is_clk","d_is_new","d_is_yuid","d_is_ref","d_cnt_ip","d_cnt_ua","d_cnt_spaceid","d_cnt_yuid","d_cnt_url","d_cnt_domain","d_cnt_ref_domain","d_cnt_os","d_cnt_brs","w_bcookie","w_entropy","w_timeGapMean","w_timeGapStd")
	colnames(inData) = colName

}

########################################## Main ##########################################

# main
main
