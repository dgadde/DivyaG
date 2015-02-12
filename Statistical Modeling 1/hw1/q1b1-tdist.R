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
N=10000;
alpha=.05;
rejectarr=array(1:20);
for (dof in 1:20){
nreject=0;
for(i in 1:N){
x1=rnorm(n1);
#x1=rnorm(n2);
x2=rt(n2,df=dof);
tstat=twosample.tstatistic(x1,x2);
if (abs(tstat)>qt(1-alpha/2, df=n1+n2-2)){
nreject=nreject+1;
}
}
significancelevel=nreject/N;
rejectarr[dof]=significancelevel;
}
plot(1:20,rejectarr, xlab="Degrees of freedom of T distribution", ylab="Type I error", main="2 sample t-statistic when one group follows a T distribution")