getHistDevice <- function(input,fea,if_log,xmax,xstep,xlim,ylim,xlab,main) {
	if( if_log==1 ) {	
		inp_tmp <- cbind(input[,c(1:2)],log(input[,fea]))
	} else {
		inp_tmp <- cbind(input[,c(1:2)],input[,fea])
	}
	ind = inp_tmp[,3] > xlim
	inp_tmp[ind,3] = xlim

	axis_y <- seq(0,xmax,by=xstep)
	axis_x <- seq(xstep,xmax,by=xstep)

	hist0 <- hist(inp_tmp[which(inp_tmp[,2]=="Desktop"),3],breaks=axis_y,plot=F);	perc0 <- hist0$counts/sum(hist0$counts)
	hist1 <- hist(inp_tmp[which(inp_tmp[,2]=="Mobile"),3],breaks=axis_y,plot=F);	perc1 <- hist1$counts/sum(hist1$counts)
	hist2 <- hist(inp_tmp[which(inp_tmp[,2]=="Tablet"),3],breaks=axis_y,plot=F);	perc2 <- hist2$counts/sum(hist2$counts)
	hist3 <- hist(inp_tmp[which(inp_tmp[,2]!="Desktop" & inp_tmp[,2]!="Mobile" & inp_tmp[,2]!="Tablet"),3],breaks=axis_y,plot=F);	perc3 <- hist3$counts/sum(hist3$counts)
	perc_tmp <- cbind(axis_x,perc0,perc1,perc2,perc3)
	perc <- perc_tmp[c(1:xlim),]

	plot(perc[,1],perc[,2], type="l", col=1, xlim=c(0,xlim), ylim=c(0,ylim), xlab=xlab,ylab="probability",main=main)
	lines(perc[,1],perc[,3],type="l",col=2)
	lines(perc[,1],perc[,4],type="l",col=3)
	lines(perc[,1],perc[,5],type="l",col=4)
	legend("topright", legend=c("Desktop","Mobile","Tablet","Others"),col=c(1:4), lty=c(1,1,1,1));
}

# input: bcookie, timeslice, fea
getHistTh0 <- function(input,fea,if_log,xmax,xstep,xlim,ylim,xlab,main) {
	if( if_log==1 ) {	
		inp_tmp <- cbind(input[,1],log(input[,fea]))
	} else {
		inp_tmp <- cbind(input[,1],input[,fea])
	}

	ind = inp_tmp[,2] > xlim
	inp_tmp[ind,2] = xlim

	axis_y <- seq(-1,xmax,by=xstep)
	axis_x <- seq(0,xmax,by=xstep)

	hist0 <- hist(inp_tmp[,2],breaks=axis_y,plot=F);	perc0 <- hist0$counts/sum(hist0$counts)
	perc_tmp <- cbind(axis_x,perc0)
	perc <- perc_tmp[c(1:(xlim+1)),]
	
	plot(perc[,1],perc[,2], type="l", col=1, xlim=c(0,xlim), ylim=c(0,ylim), xlab=xlab,ylab="probability",main=main)
}

getHistTh = function(input,fea,if_log,if_zero,xmax,xstep,xlim,ylim,if_table,xlab,main) {
	if( if_log==1 ) {
		inp_tmp <- cbind(input[,1],log(input[,fea]))
	} else {
		inp_tmp <- cbind(input[,1],input[,fea])
	}
	
	ind = inp_tmp[,2] > xlim
	inp_tmp[ind,2] = xlim
	
	axis_y <- seq(-xstep,xmax,by=xstep)
	axis_x <- seq(0,xmax,by=xstep)
	
	hist0 <- hist(inp_tmp[,2],breaks=axis_y,plot=F);        perc0 <- hist0$counts/sum(hist0$counts)
	perc_tmp <- cbind(axis_x,perc0)
	
	if( if_zero==1 ) {
		perc <- perc_tmp[c(1:(xlim/xstep+1)),]
	}else {
		perc <- perc_tmp[c(2:(xlim/xstep+1)),]
	}
	
	plot(perc, type="l", col=1, xlim=c(0,xlim), ylim=c(0,ylim), xlab=xlab,ylab="probability",main=main)
	
	if(if_table==1) {
		return(cbind(perc_tmp[,1],cumsum(perc_tmp[,2])))
	}
}
