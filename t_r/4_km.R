datapath <- "/tmp/wm/abf_case_BRpeak3_paper/4_km/"
Args <- commandArgs(TRUE)
ts <- as.numeric(Args[1])


################## Func ##################

nor_log<-function(vec){a=min(log(vec+1));b=max(log(vec+1));c=(log(vec+1)-a)/(b-a);return(c)} 
data_process<-function(vec){c=nor_log(vec); return(c)} 

maxmin_log<-function(vec){a=min(log(vec+1));b=(max(log(vec+1))-min(log(vec+1)));c=c(a,b);return(c)}
maxmin_process<-function(vec){c=maxmin_log(vec); return(c)} 

################## Load ##################

matrix0 = read.table(paste(datapath,ts,"/1_sus",sep=""), na.strings = "NA", sep = "\t")
matrix1 = matrix0[,c(2,6:15)]
# get max min
maxmin_matrix<-apply(matrix1,2,maxmin_process)
# normalize
matrix1<-apply(matrix1,2,data_process)
matrix2 = matrix0[,c(3:5)]
matrix = cbind(matrix1,matrix2)

################## Cluster ##################

#cluster_cnt = nrow(matrix)/10
cluster_cnt = 5
km = kmeans(matrix, cluster_cnt, 50)

################## Output ##################

write.table(km$center, file = paste(datapath,ts,"/center",sep = ""), sep = "\t", row.names = FALSE, quote = FALSE, col.names = FALSE)
write.table(km$cluster, file = paste(datapath,ts,"/cluster", sep = ""), sep = "\t", row.names = FALSE, quote = FALSE, col.names = FALSE)
write.table(maxmin_matrix, file = paste(datapath,ts,"/maxmin", sep = ""), sep = "\t", row.names = FALSE, quote = FALSE, col.names = FALSE)

