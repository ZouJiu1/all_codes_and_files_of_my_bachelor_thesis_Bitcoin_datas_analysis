#查找一下实体
import pandas as pd
import numpy as np
import os
import gc
import sys
path=r'E:\extract\2'
dirs=os.listdir(path)
for i in dirs:
    # entity2=pd.DataFrame(columns=['Addr','Num'])
    # entity2['Addr'] =['123456']
    # entity2['Num'] = 0
    # entity2.to_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]),index=False)
    entity2=pd.read_csv(r'E:\entity\entity427001-430000.csv')
    PATH=os.path.join(path,i)
    df=pd.read_csv(PATH,usecols=['Input'])
    print(PATH,len(df),end='      ')
    Add={'Addr':[],'Num':[]}
    for ind in df.index:
        lisa=df.loc[ind,'Input'].replace('[','').replace(']','').replace('\'','').replace(',','').split(' ')
        mark=0
        set_lisa=set(lisa)
        val=list(entity2['Addr'].values)
        val.extend(Add['Addr'])
        set_value=set(val)
        join=set_lisa&set_value
        if join==set():
            if Add['Num']==[]:
                MAX = [entity2['Num'].max()+1] * len(lisa)
            else:
                MAX = [max(Add['Num'])+1] * len(lisa)
        else:
            common = list(join)[0]
            if common in Add['Addr']:
                INDEX = Add['Addr'].index(list(join)[0])
                MAX = [Add['Num'][INDEX]] * len(lisa)
            else:
                MAX = [entity2[entity2['Addr'] == common]['Num'].max()] * len(lisa)
        Add['Addr'].extend(lisa)
        Add['Num'].extend(MAX)
        if (ind%16000==0) and (ind!=0):
            ADD = pd.DataFrame(Add)
            entity2 = pd.concat([entity2, ADD], axis=0)
            entity2.drop_duplicates(subset=['Addr','Num'],inplace=True)
            entity2.to_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]),index=False)
            df.drop(list(np.arange(df.index[0], ind + 1, 1)), axis=0, inplace=True)
            df.to_csv(r'e:\temp\temp2.csv')
            del df, Add, val, set_value, ADD, entity2
            gc.collect()
            import time
            time.sleep(6)
            gc.collect()
            Add = {'Addr': [], 'Num': []}
            entity2=pd.read_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]))
            df=pd.read_csv(r'e:\temp\temp2.csv',index_col=0)
            print(round(float((ind/len(df))*100),3),'%','ind:',ind,end=' ')
            with open(r'E:\entity\txt%s.txt' % (i.split('.')[0]),'a+') as obj:
                obj.write(r'百分之%s ind：%s  ' % (round(float((ind / len(df)) * 100), 3), ind))
    ADD = pd.DataFrame(Add)
    entity2 = pd.concat([entity1, ADD], axis=0)
    entity2.drop_duplicates(subset=['Addr', 'Num'], inplace=True)
    entity2.to_csv(r'E:\entity\entity%s.csv' % (i.split('.')[0]),index=False)
    del df
    gc.collect()
    import time
    time.sleep(10)
    gc.collect()
    print('\nDone!',i)