#查找一下实体
import pandas as pd
import numpy as np
import os
import gc
path=r'E:\extract\1'
dirs=os.listdir(path)
entity1=pd.read_csv(r'E:\entity\entity1.csv')
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
            if st not in entity1['Addr']:
                continue
            else:
                mark=100
                SAMENUM=entity1[entity1['Addr']==st]['Num'].max()
                break
        if mark==0:
            MAX=[entity1['Num'].max()+1]*len(lisa)
            Add={'Addr':lisa,'Num':MAX}
            ADD=pd.DataFrame(Add)
            entity1=pd.concat([entity1,ADD],axis=0)
        if mark==100:
            MAX=[SAMENUM]*len(lisa)
            Add={'Addr':lisa,'Num':MAX}
            ADD=pd.DataFrame(Add)
            entity1=pd.concat([entity1,ADD],axis=0)
            entity1.drop_duplicates(subset=['Addr','Num'],inplace=True)
    j+=1
    entity1.to_csv(r'E:\entity\entity1.csv',index=False)
    del df
    gc.collect()
    import time
    time.sleep(6)
    print('Done!','------',(j*100)/len(dirs),'%')