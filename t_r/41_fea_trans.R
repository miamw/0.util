######################## Func ########################

norm1 = function(x) {
  y=(x-mean(x))/sd(x)
  
  return(y)
}

norm2 = function(x) {
  y=(x-min(x))/(max(x)-min(x))
  
  return(y)
}

getHist1 = function(input,if_log,xmax,xstep,xlim,ylim,xlab,main) {
  
  if( if_log==1 ) {	
    inp = cbind(input[,1],log(input[,2]))
  } else {
    inp = input[,c(1,2)]
  }
  
  # Deal with max & min features
  ind = inp[,2] > xlim;	inp[ind,2] = xlim;
  ind = inp[,2] < -xlim;  inp[ind,2] = -xlim;
  
  breaks = seq(-xmax,xmax,by=xstep)
  axis_x = seq(-xmax+xstep,xmax,by=xstep)
  
  hist0 = hist(inp[which(inp[,1]==0),2],breaks=breaks,plot=F);	perc0 = hist0$counts/sum(hist0$counts)
  hist1 = hist(inp[which(inp[,1]==1),2],breaks=breaks,plot=F);	perc1 = hist1$counts/sum(hist1$counts)
  perc = cbind(axis_x,perc0,perc1)

  plot(perc[,1],perc[,2], type="l", col=1, xlim=c(-xlim,xlim), ylim=c(0,ylim), xlab=xlab,ylab="probability",main=main)
  lines(perc[,1],perc[,3],type="l",col=2)
  legend("topright", legend=c("Negative","Positive"),col=c(1:2), lty=c(1,1));
}

getHist2 = function(input,if_log,if_zero,xmax,xstep,xlim,ylim,xlab,main) {

	if( if_log==1 ) {	
		inp = cbind(input[,1],log(input[,2]))
	} else {
		inp = input[,c(1,2)]
	}
	
	# Deal with max & min features
	ind = inp[,2] > xlim;	inp[ind,2] = xlim;	comp = inp[which(inp[,2]>=0),];

	if( if_zero==1 ) {
		axis_y = seq(-xstep,xmax,by=xstep)
		axis_x = seq(0,xmax,by=xstep)
	} else {
		axis_y = seq(0,xmax,by=xstep)
		axis_x = seq(xstep,xmax,by=xstep)
	}

	hist0 = hist(comp[which(comp[,1]==0),2],breaks=axis_y,plot=F);	perc0 = hist0$counts/sum(hist0$counts)
	hist1 = hist(comp[which(comp[,1]==1),2],breaks=axis_y,plot=F);	perc1 = hist1$counts/sum(hist1$counts)
	perc_tmp = cbind(axis_x,perc0,perc1)
	if( if_zero==1 ) {
		perc = perc_tmp[c(1:(xlim/xstep+1)),]
	} else {
		perc = perc_tmp[c(1:xlim/xstep),]
	}

	plot(perc[,1],perc[,2], type="l", col=1, xlim=c(0,xlim), ylim=c(0,ylim), xlab=xlab,ylab="probability",main=main)
	lines(perc[,1],perc[,3],type="l",col=2)
	legend("topright", legend=c("Negative","Positive"),col=c(1:2), lty=c(1,1));
}

getChartList1 = function(x) {
  inp=cbind(input[,1],norm1(input[,x]))
  getHist1(input=inp,if_log=0,  xmax=0.5,	xstep=0.1, xlim=0.5,	ylim=1,	xlab=x,	main=x) 
}

getChartList2 = function(x) {
  inp=cbind(input[,1],norm2(input[,x]))
  getHist2(input=inp,if_log=0,if_zero=1, xmax=0.5,  xstep=0.1, xlim=0.5,	ylim=1,	xlab=x,	main=x) 
}

######################## Main ########################

datapath="/Users/mengwang/Work/R/"

## Load
#input = read.table(paste(datapath,"1_feature.100",sep=""),header=T, na.strings="NA",sep="\t",comment.char="")

#################### Fea

### 1

pdf(file="/Users/mengwang/Work/R/dist1.pdf")

for (x in 3:length(input)) {
  getChartList1(x)
}

dev.off()

#### 2

pdf(file="/Users/mengwang/Work/R/dist2.pdf")

for (x in 3:length(input)) {
  getChartList2(x)
}

dev.off()

