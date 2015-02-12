import numpy as np
from copy import deepcopy

data=np.genfromtxt("trimmed_training_10_10.txt",delimiter=' ',dtype=int)
testdata=np.genfromtxt("trimmed_test_1010_500_topN.txt",delimiter=' ',dtype=int, skip_header=1)

item_adj_list=dict.fromkeys(data[:,1],None)


for i in item_adj_list.keys():
    item_adj_list[i]=[]

for i in range(data.shape[0]):
    item_adj_list[data[i][1]].append((data[i][0],data[i][2]))
    
user_adj_list=dict.fromkeys(data[:,0],None)

for i in user_adj_list.keys():
    user_adj_list[i]=[]

for i in range(data.shape[0]):
    user_adj_list[data[i][0]].append((data[i][1],data[i][2]))
    
user_averages=dict.fromkeys(data[:,0],0)

for i in user_averages.keys():
   user_averages[i]=np.average(list(zip(*user_adj_list[i]))[1])

item_averages=dict.fromkeys(data[:,1],0)

for i in item_averages.keys():
   item_averages[i]=np.average(list(zip(*item_adj_list[i]))[1])

trustdata=np.genfromtxt("data/trust_data 2.txt",delimiter=' ',dtype=int)
trust_adj_list=dict.fromkeys(trustdata[:,0],None)
list_trust_size = trustdata.shape[0]
for i in trust_adj_list.keys():
    trust_adj_list[i]=[]

for i in range(list_trust_size):
        trust_adj_list[trustdata[i][0]].append(trustdata[i][1])

import math
def pearson_corr_user(i,j):
    if not(i in user_adj_list) or not(j in user_adj_list):  
        return -2
    itemini= list(zip(*user_adj_list[i]))[0]
    iteminj= list(zip(*user_adj_list[j]))[0]
    commonitems = filter(set(itemini).__contains__, iteminj)
    commonitems = filter(set(commonitems).__contains__, user_adj_list.keys())
    useriratings=[]
    userjratings=[]
    citems = len(commonitems)
    if (citems == 0):
        return -2
    for l in range(citems):
       useriratings.append(user_adj_list[i][itemini.index(commonitems[l])][1]-item_averages[commonitems[l]])
       userjratings.append(user_adj_list[j][iteminj.index(commonitems[l])][1]-item_averages[commonitems[l]])
    num=np.dot(useriratings,userjratings)
    deno1=np.sqrt(np.dot(useriratings,useriratings))
    deno2=np.sqrt(np.dot(userjratings,userjratings))
    deno = deno1*deno2 *(1+math.exp(-((citems*1.0)/2)))
    if (deno !=0):
        return ( num*1.0/deno) 
    else:
        return -2

user=56 
N=100
def predicttopN(user,item):
    top_N_items= dict()
    for i in trust_adj_list[user]:
        if not(user_adj_list.has_key(i)):
            continue;
        if(pearson_corr_user(user,i)<.5):
            continue
        itemsrated=list(zip(*user_adj_list[i]))[0]
        ratings=list(zip(*user_adj_list[i]))[1]
        m = max(ratings)
        item_list = [ii for ii, j in enumerate(ratings) if j >= m-1]
        for k in item_list:
            if top_N_items.has_key(itemsrated[k]):
                top_N_items[itemsrated[k]].append(ratings[k]-user_averages[i])
            else:
                top_N_items[itemsrated[k]]=[]
                top_N_items[itemsrated[k]].append(ratings[k]-user_averages[i])
        if not(trust_adj_list.has_key(i)):
                continue;
        for p in trust_adj_list[i]:
            if not(user_adj_list.has_key(p)):
                continue;
            if(pearson_corr_user(user,p)<.5):
                continue
            itemsrated=list(zip(*user_adj_list[p]))[0]
            ratings=list(zip(*user_adj_list[p]))[1]
            m = max(ratings)
            item_list = [ii for ii, j in enumerate(ratings) if j >= m-1]
            for k in item_list:
                if top_N_items.has_key(itemsrated[k]):
                    top_N_items[itemsrated[k]].append(ratings[k]-user_averages[p])
                else:
                    top_N_items[itemsrated[k]]=[]
                    top_N_items[itemsrated[k]].append(ratings[k]-user_averages[p])

    numitems=len(top_N_items.keys())
    top_items_avgs=np.zeros((numitems,2) ) 
    cc=0
    for i in top_N_items.keys():
        top_items_avgs[cc][0]=i
        top_items_avgs[cc][1]=np.mean(top_N_items[i])
        cc=cc+1
    if (numitems<=N):
        return sum(top_items_avgs[:,0]==item)
    else:
        top100=top_items_avgs[np.argsort(top_items_avgs[:,1])[range(numitems-N,numitems)]]
        return sum(top100[:,0]==item)


recallcount=0
denom=0
for i in range(testdata.shape[0]):
    if not(user_adj_list.has_key(testdata[i][0])):
            continue;
    if not(trust_adj_list.has_key(testdata[i][0])):
            continue;
    recallcount=recallcount+predicttopN(testdata[i][0],testdata[i][1])
    denom=denom+1
print recallcount*1.0/denom 





