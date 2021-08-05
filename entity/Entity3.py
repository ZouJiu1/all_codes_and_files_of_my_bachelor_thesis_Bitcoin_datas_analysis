#查找一下实体
import pandas as pd
import numpy as np
import os
import gc
path=r'E:\extract\3'
dirs=os.listdir(path)
entity3=pd.read_csv(r'E:\entity\entity3.csv')
j=0
for i in dirs:
    PATH=os.path.join(path,i)
    print(PATH,end='      ')
    df=pd.read_csv(PATH,usecols=['Input'])
    for ind in df.index:
        string=df.loc[ind,'Input']
        string=string.replace('[','').replace(']','').replace('\'','').replace(',','')
        lisa=string.split(' ')
        mark=0
        for st in lisa:
            if st not in entity3['Addr']:
                continue
            else:
                mark=100
                SAMENUM=entity3[entity3['Addr']==st]['Num'].max()
                break
        if mark==0:
            MAX=[entity3['Num'].max()+1]*len(lisa)
            Add={'Addr':lisa,'Num':MAX}
            ADD=pd.DataFrame(Add)
            entity3=pd.concat([entity3,ADD],axis=0)
        if mark==100:
            MAX=[SAMENUM]*len(lisa)
            Add={'Addr':lisa,'Num':MAX}
            ADD=pd.DataFrame(Add)
            entity3=pd.concat([entity3,ADD],axis=0)
            entity3.drop_duplicates(subset=['Addr','Num'],inplace=True)
    j+=1
    entity3.to_csv(r'E:\entity\entity3.csv',index=False)
    del df
    gc.collect()
    import time
    time.sleep(6)
    print('Done!', '------', (j * 100) / len(dirs), '%')