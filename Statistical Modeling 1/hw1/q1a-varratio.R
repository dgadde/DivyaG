twosample.tstatistic=function(x,y){
n1=length(x);
n2=length(y);
x1bar=mean(x);
x2bar=mean(y);
s1=sd(x);
s2=sd(y);
spsquared=((n1-1)*(s1^2)+(n2-1)*(s2^2))/(n1+n2-2);
tstat=(x1bar-x2bar)/(sqrt((spsquared/n1)+(spsquared/n2)));
return(tstat);
}

#MC simulation to test robustness of 2 sample t statistic
n=15;
n1=n;
n2=n;
N=100000;
alpha=.05;
sddratio=seq(1,5,1)
iters=length(sddratio);
rejectarr=array(1:iters);
for (i in 1:iters){
nreject=0;
for(jj in 1:N){
x1=rnorm(n1);
x2=rnorm(n2)*sddratio[i];
#x2=rt(n2,df=dof);
tstat=twosample.tstatistic(x1,x2);
if (abs(tstat)>qt(1-alpha/2, df=n1+n2-2)){
nreject=nreject+1;
}
}
significancelevel=nreject/N;
rejectarr[i]=significancelevel;
}
plot(sddratio,rejectarr, xlab="Ratio of Standard deviations", ylab="Type I error", main="2 sample t-statistic when variance of the 2 groups is unequal")