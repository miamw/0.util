pdf(file="/homes/qiuxe/apache-tomcat-7.0.30/webapps/ROOT/wm/4_class_dist.pdf")
datapath="/tmp/wm/abf_case_BRpeak3_paper/"
Args <- commandArgs(TRUE)
ts <- as.numeric(Args[1])

in_sus <- read.table(paste(datapath,"4_km/",ts,"/1_sus.cluster",sep=""), na.strings = "NA", sep = "\t")
in_ttl <- read.table(paste(datapath,"4_km/",ts,"/2_ttl",sep=""), na.strings = "NA", sep = "\t")

################### Func ###################

norm_prob <- function(vec) {
	vec<-(vec+1)/(sum(vec)+length(vec))
	return(vec)
}

kld <- function(vec1,vec2) {
	dist1 <- sum(vec1*log(vec1/vec2))
	dist2 <- sum(vec2*log(vec2/vec1))
	dist <- mean(c(dist1,dist2))
	return(dist)
}

fea_kld <- function(fld,main,islog) {
	if(islog==1) {
		axis_y <- seq(0,14,by=0.5)
		axis_x <- seq(0.5,14,by=0.5)

		ip0 <- in_ttl[,fld]
		ip_h0 <- hist(log(ip0), plot=F,breaks=axis_y)
		cnt0 <- ip_h0$count
		ip1 <- in_sus[which(in_sus[,16]==1),fld]
		ip_h1 <- hist(log(ip1), plot=F,breaks=axis_y)
		cnt1 <- ip_h1$count
		ip2 <- in_sus[which(in_sus[,16]==2),fld]
		ip_h2 <- hist(log(ip2), plot=F,breaks=axis_y)
		cnt2 <- ip_h2$count
		ip3 <- in_sus[which(in_sus[,16]==3),fld]
		ip_h3 <- hist(log(ip3), plot=F,breaks=axis_y)
		cnt3 <- ip_h3$count
		ip4 <- in_sus[which(in_sus[,16]==4),fld]
		ip_h4 <- hist(log(ip4), plot=F,breaks=axis_y)
		cnt4 <- ip_h4$count
		ip5 <- in_sus[which(in_sus[,16]==5),fld]
		ip_h5 <- hist(log(ip5), plot=F,breaks=axis_y)
		cnt5 <- ip_h5$count
	
		# chart
		plot(axis_x,cnt0/sum(cnt0),type="l",col=1,xlim=c(0,10),ylim=c(0,0.8),xlab=main,ylab="density",main=main)
		lines(axis_x,cnt1/sum(cnt1),type="l",col=2)
		lines(axis_x,cnt2/sum(cnt2),type="l",col=3)
		lines(axis_x,cnt3/sum(cnt3),type="l",col=4)
		lines(axis_x,cnt4/sum(cnt4),type="l",col=5)
		lines(axis_x,cnt5/sum(cnt5),type="l",col=6)
		legend("topright", legend=c("Overall","Class1","Class2","Class3","Class4","Class5"),col=c(1,2,3,4,5,6), lty=c(1,1,1,1,1,1));
	}else{
		axis_y <- seq(0,1,by=0.05)
		axis_x <- seq(0.05,1,by=0.05)
				
		ip0 <- in_ttl[,fld]
		ip_h0 <- hist(ip0, plot=F,breaks=axis_y)
		cnt0 <- ip_h0$count
		ip1 <- in_sus[which(in_sus[,16]==1),fld]
		ip_h1 <- hist(ip1, plot=F,breaks=axis_y)
		cnt1 <- ip_h1$count
		ip2 <- in_sus[which(in_sus[,16]==2),fld]
		ip_h2 <- hist(ip2, plot=F,breaks=axis_y)
		cnt2 <- ip_h2$count
		ip3 <- in_sus[which(in_sus[,16]==3),fld]
		ip_h3 <- hist(ip3, plot=F,breaks=axis_y)
		cnt3 <- ip_h3$count
		ip4 <- in_sus[which(in_sus[,16]==4),fld]
		ip_h4 <- hist(ip4, plot=F,breaks=axis_y)
		cnt4 <- ip_h4$count
		ip5 <- in_sus[which(in_sus[,16]==5),fld]
		ip_h5 <- hist(ip5, plot=F,breaks=axis_y)
		cnt5 <- ip_h5$count

		# chart
		plot(axis_x,ip_h0$count/sum(ip_h0$count),type="l",col=1,xlim=c(0,1),ylim=c(0,1),xlab=main,ylab="density",main=main)
		lines(axis_x,cnt1/sum(cnt1),type="l",col=2)
		lines(axis_x,cnt2/sum(cnt2),type="l",col=3)
		lines(axis_x,cnt3/sum(cnt3),type="l",col=4)
		lines(axis_x,cnt4/sum(cnt4),type="l",col=5)
		lines(axis_x,cnt5/sum(cnt5),type="l",col=6)
		legend("topright", legend=c("Overall","Class1","Class2","Class3","Class4","Class5"),col=c(1,2,3,4,5,6), lty=c(1,1,1,1,1,1));
	}
	
	# KL
	prob0 <- norm_prob(cnt0)
	prob1 <- norm_prob(cnt1)
	prob2 <- norm_prob(cnt2)
	prob3 <- norm_prob(cnt3)
	prob4 <- norm_prob(cnt4)
	prob5 <- norm_prob(cnt5)
	
	c1 <- kld(prob0,prob1)
	c2 <- kld(prob0,prob2)
	c3 <- kld(prob0,prob3)
	c4 <- kld(prob0,prob4)
	c5 <- kld(prob0,prob5)
	c <- c(c1,c2,c3,c4,c5)
	
	return(c)
}

################### Debug ###################

d<-1

################### Main ###################

f1<-fea_kld(2,"ttl_trf",1)
f2<-fea_kld(6,"uid_cnt",1)
f3<-fea_kld(7,"avg_uid_cnt",1)
f4<-fea_kld(9,"avg_ua_cnt",1)
f5<-fea_kld(10,"spaceid_cnt",1)
f6<-fea_kld(11,"avg_spaceid_cnt",1)
f7<-fea_kld(12,"active_hour",1)
f8<-fea_kld(13,"peek_hour",1)
f9<-fea_kld(14,"active_5m",1)
f10<-fea_kld(15,"peek_5m",1)
f11<-fea_kld(3,"new_trf_rate",0)
f12<-fea_kld(4,"new_u_rate",0)
f13<-fea_kld(5,"login_trf_rate",0)

f<-cbind(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13)
fsum<-c(sum(f[1,]),sum(f[2,]),sum(f[3,]),sum(f[4,]),sum(f[5,]))

dev.off();

write.table(f, file = paste(datapath,"5_kl/",ts,"/1_kls",sep = ""), sep = "\t", row.names = FALSE, quote = FALSE, col.names = FALSE)
write.table(fsum, file = paste(datapath,"5_kl/",ts,"/2_sum",sep = ""), sep = "\t", row.names = FALSE, quote = FALSE, col.names = FALSE)

################### Debug ###################

#d
