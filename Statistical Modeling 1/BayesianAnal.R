library(coda)
library(lattice)
library(boot)
library(R2WinBUGS)
data=read.table('wtloss.txt', header=T)
attach(data)
head(data)
N=nrow(data)
race.f = factor(race)
dummies = model.matrix(~race.f)
dum1=dummies[,2]
dum2=dummies[,3]
dum3=dummies[,4]
trt.f = factor(treatment)
trtdummies = model.matrix(~trt.f)
trt1=trtdummies[,2]
trt2=trtdummies[,3]
inits = function(){list(beta = rep(0,8), tau = 1)}
databug = list(wtch=wtch, age=age,r1=dum1,r2=dum2,r3=dum3, trt1=trt1,trt2=trt2,bio=biomarker,N=N)
datacov=cbind(rep(1,N),dum1,dum2,dum3,trt1, trt2,age,biomarker)
mymodel = function(){
  for(i in 1:N){
    wtch[i]~dnorm(mu[i],tau)
    mu[i]<-beta[1]+beta[2]*r1[i]+beta[3]*r2[i]+beta[4]*r3[i]+beta[5]*trt1[i]+beta[6]*trt2[i]+beta[7]*age[i]+beta[8]*bio[i]
  }
  for(i in 1:8){
    beta[i]~dnorm(0,0.0001) # Diffuse Priors
  }
  tau~dgamma(0.001, 0.001)
  
}


write.model(mymodel, "model5.txt")
themodel = paste(getwd(),"model5.txt",sep="/")
param = c("beta", "tau")
hmodel2 <- bugs(data = databug, inits = inits, parameters.to.save = param,
               model.file = "model5.txt", n.chains = 3, n.iter = 10000,n.burnin = 1000,
               n.thin = 10, bugs.directory = "C:/Program/WinBUGS14",
               working.directory="C:/Users/Divya/Documents/Coursework/Statistical Modelling 1/Project",debug=TRUE)

attach.bugs(hmodel2)
print(hmodel2,digits.summary = 3)

CPO = matrix(0, N, 2700)


for(i in 1:N){
  for(j in 1:2700){
    CPO[i,j] = 1/dnorm(wtch[i], mean = t(as.matrix(datacov[i,])) %*% as.matrix(beta[j,]), sd = sqrt(1/tau[j]))
  }
}
LPML = sum(log(1/rowMeans(CPO)))

full.fit = lm(data$wtch ~ dum1+dum2+dum3+trt1+trt2+data$age+data$biomarker)
cutoff = 2 * sd(full.fit$resid)
k.obs = sum(abs(full.fit$resid) > cutoff)
T.obs = sum(rstudent(full.fit)^2)
Yrep = matrix(0, N, 2700)
for(i in 1:N){
  for(j in 1:2700){
    Yrep[i,j] = rnorm(1, mean = t(as.matrix(datacov[i,])) %*% as.matrix(beta[j,]), sd = sqrt(1/tau[j]))
  }
}
k.rep = rep(0, 2700)
T.rep = rep(0, 2700)
for(i in 1:2700){
  fit = lm(Yrep[ ,i] ~ dum2+data$age)
  k.rep[i] = sum(abs(fit$resid) > cutoff)
  T.rep[i] = sum(rstudent(fit)^2)
}
hist(k.rep)
abline(v = k.obs)
length(k.rep[k.rep >= k.obs])/length(k.rep)
hist(T.rep)
abline(v = T.obs)
length(T.rep[T.rep >= T.obs])/length(T.rep)
