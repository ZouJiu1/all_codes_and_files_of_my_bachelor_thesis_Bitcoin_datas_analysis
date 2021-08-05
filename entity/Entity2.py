#查找一下实体
import pandas as pd
import numpy as np
import os
import gc
import sys
path=r'E:\extract\2'
dirs=os.listdir(path)
for i in dirs:
    entity2=pd.DataFrame(columns=['Addr','Num'])
    entity2['Addr'] =['123456']
    entity2['Num'] = 0
    entity2.to_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]),index=False)
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
            if st not in entity2['Addr']:
                continue
            else:
                mark=100
                SAMENUM=entity2[entity2['Addr']==st]['Num'].max()
                break
        if mark==0:
            MAX=[entity2['Num'].max()+1]*len(lisa)
            Add={'Addr':lisa,'Num':MAX}
            ADD=pd.DataFrame(Add)
            entity2=pd.concat([entity2,ADD],axis=0)
        if mark==100:
            MAX=[SAMENUM]*len(lisa)
            Add={'Addr':lisa,'Num':MAX}
            ADD=pd.DataFrame(Add)
            entity2=pd.concat([entity2,ADD],axis=0)
        if ind%16000==0:
            entity2.drop_duplicates(subset=['Addr','Num'],inplace=True)
            entity2.to_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]),index=False)
            temp=ind
            del df
            del Add
            del ADD
            del entity2
            gc.collect()
            import time
            time.sleep(6)
            gc.collect()
            entity2=pd.read_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]))
            df=pd.read_csv(PATH,usecols=['Input'])
            print(round(float((ind/len(df))*100),3),'%','ind:',ind,end=' ')
            with open(r'E:\entity\txt%s.txt' % (i.split('.')[0]),'a+') as obj:
                obj.write(r'%s|  ind：%s  ' % (round(float((ind / len(df)) * 100), 3), ind))
    entity2.to_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]),index=False)
    del df
    gc.collect()
    import time
    time.sleep(10)
    print('\nDone!',i)