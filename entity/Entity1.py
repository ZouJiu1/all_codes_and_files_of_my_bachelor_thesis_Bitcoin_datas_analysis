#查找一下实体
import pandas as pd
import numpy as np
import os
import gc
import sys
path=r'E:\extract\1'
dirs=os.listdir(path)
for i in dirs:
    entity1=pd.DataFrame(columns=['Addr','Num'])
    entity1['Addr']=['123456']
    entity1['Num']=0
    entity1.to_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]),index=False)
    PATH=os.path.join(path,i)
    df=pd.read_csv(PATH,usecols=['Input'])
    print(PATH,len(df),end='      ')
    # start=input('开始的索引：')
    for ind in df.index:
        # if ind<int(start):
        #     continue
        lisa=df.loc[ind,'Input'].replace('[','').replace(']','').replace('\'','').replace(',','').split(' ')
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
        if ind%30000==0:
            entity1.drop_duplicates(subset=['Addr','Num'],inplace=True)
            entity1.to_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]),index=False)
            temp=ind
            del df
            del Add
            del ADD
            del entity1
            gc.collect()
            import time
            time.sleep(6)
            gc.collect()
            entity1=pd.read_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]))
            df=pd.read_csv(PATH,usecols=['Input'])
            print(round(float((ind/len(df))*100),3),'%','ind:',ind,end=' ')
            with open(r'E:\entity\txt%s.txt' % (i.split('.')[0]),'a+') as obj:
                obj.write(r'%s|   ind：%s  ' % (round(float((ind / len(df)) * 100), 3), ind))
    entity1.to_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]),index=False)
    del df
    gc.collect()
    import time
    time.sleep(10)
    print('\nDone!',i)