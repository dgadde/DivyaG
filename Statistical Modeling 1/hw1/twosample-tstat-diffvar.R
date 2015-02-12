twosample.tstatistic2=function(x,y){
n1=length(x);
n2=length(y);
x1bar=mean(x);
x2bar=mean(y);
s1=sd(x);
s2=sd(y);
t1=(s1^2)/n1;
t2=(s2^2)/n2;
t1squared=t1^2;
t2squared=t2^2;
tstat=(x1bar-x2bar)/sqrt(t1+t2);
dof=round(((t1+t2)^2)/((t1squared/(n1-1)) + (t2squared/(n2-1))));
return(c(tstat,dof));
}
