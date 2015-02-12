> install.packages("LearnBayes")
> library("LearnBayes")
> data(studentdata)
> hist(studentdata$Dvds)
> summary(studentdata$Dvds)


> attach(studentdata)
> plot(ToSleep, WakeUp, main="Scatterplot of wake up and to sleep times")
> fit=lm(WakeUp~ToSleep)
> abline(fit)
> summary(fit)

Call:
lm(formula = WakeUp ~ ToSleep)

Residuals:
    Min      1Q  Median      3Q     Max 
-4.4010 -0.9628 -0.0998  0.8249  4.6125 

Coefficients:
            Estimate Std. Error t value Pr(>|t|)    
(Intercept)  7.96276    0.06180  128.85   <2e-16 ***
ToSleep      0.42472    0.03595   11.81   <2e-16 ***
---
Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1

Residual standard error: 1.282 on 651 degrees of freedom
  (4 observations deleted due to missingness)
Multiple R-squared:  0.1765,    Adjusted R-squared:  0.1753 
F-statistic: 139.5 on 1 and 651 DF,  p-value: < 2.2e-16

