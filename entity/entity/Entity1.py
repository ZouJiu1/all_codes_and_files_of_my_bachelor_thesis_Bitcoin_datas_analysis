train = train.sort_values(by=['day', 'user_id'])  # 同一天同一个用户的点击广告的次数——--用户的搜索次数和点击次数
h = train.groupby(by=['day', 'user_id']).count()
lis = []
for i, j in h.index:
    lis.extend([h.loc[i].loc[j]['user_gender_id']] * h.loc[i].loc[j]['user_gender_id'])
train['num_click'] = lis

train = train.sort_values(by=['day', 'item_id'])  # 同一天同一件商品被搜索点击次数-----商品的被搜索次数和受欢迎程度
h = train.groupby(by=['day', 'item_id']).count()
lis = []
for i, j in h.index:
    lis.extend([h.loc[i].loc[j]['user_gender_id']] * h.loc[i].loc[j]['user_gender_id'])
train['num_show'] = lis

train = train.sort_values(by=['item_id', 'context_timestamp'])  # 同一件商品被搜索点击的时间间隔
train.index = np.arange(len(train))
lis = []
for val in set(train['item_id'].values):
    lis.append(train[train['item_id'] == val].index[-1])
temp = train['context_timestamp'].diff()
for i in lis:
    temp.loc[i + 1] = 0
temp.loc[0] = 0
train['item_time_diff'] = temp

train = train.sort_values(by=['day', 'item_id', 'context_timestamp'])  # 同一天同一件商品的被搜索点击的时间间隔
train.index = np.arange(len(train))
lis = []
for d in [17, 18, 19, 20, 21, 22, 23, 24]:
    for val in set(train[train['day'] == d]['item_id'].values):
        lis.append(train[(train['item_id'] == val) & (train['day'] == d)].index[-1])
temp = train['context_timestamp'].diff()
for i in lis:
    temp.loc[i + 1] = 0
temp.loc[0] = 0
train['day_item_diff'] = temp

train = train.sort_values(by=['day', 'user_id', 'context_timestamp'])  # 同一天相同用户的不同商品点击时间间隔
train.index = np.arange(len(train))
lis = []
for d in [17, 18, 19, 20, 21, 22, 23, 24]:
    for val in set(train[train['day'] == d]['user_id'].values):
        lis.append(train[(train['user_id'] == val) & (train['day'] == d)].index[-1])
temp = train['context_timestamp'].diff()
for i in lis:
    temp.loc[i + 1] = 0
temp.loc[0] = 0
train['day_user_diff'] = temp
train.to_csv(r'D:\Data\IJCAI\no_fixed\train_pre1.csv', index=False)
print('Train has being Done!')
##########################################################################################################################################
test = test.sort_values(by=['day', 'user_id'])  # 同一天同一个用户的点击广告的次数——--用户的搜索次数和点击次数
h = test.groupby(by=['day', 'user_id']).count()
lis = []
for i, j in h.index:
    lis.extend([h.loc[i].loc[j]['user_gender_id']] * h.loc[i].loc[j]['user_gender_id'])
test['num_click'] = lis

test = test.sort_values(by=['day', 'item_id'])  # 同一天同一件商品被搜索点击次数-----商品的被搜索次数和受欢迎程度
h = test.groupby(by=['day', 'item_id']).count()
lis = []
for i, j in h.index:
    lis.extend([h.loc[i].loc[j]['user_gender_id']] * h.loc[i].loc[j]['user_gender_id'])
test['num_show'] = lis

test = test.sort_values(by=['item_id', 'context_timestamp'])  # 同一件商品被搜索点击的时间间隔
test.index = np.arange(len(test))
lis = []
for val in set(test['item_id'].values):
    lis.append(test[test['item_id'] == val].index[-1])
temp = test['context_timestamp'].diff()
for i in lis:
    temp.loc[i + 1] = 0
temp.loc[0] = 0
test['item_time_diff'] = temp

test = test.sort_values(by=['day', 'item_id', 'context_timestamp'])  # 同一天同一件商品的被搜索点击的时间间隔
test.index = np.arange(len(test))
lis = []
for val in set(test[test['item_id'].values):
    lis.append(test[test['item_id'] == val].index[-1])
temp = test['context_timestamp'].diff()
for i in lis:
    temp.loc[i + 1] = 0
temp.loc[0] = 0
test['day_item_diff'] = temp

test = test.sort_values(by=['day', 'user_id', 'context_timestamp'])  # 同一天相同用户的不同商品点击时间间隔
test.index = np.arange(len(test))
lis = []
for val in set(test['user_id'].values):
    lis.append(test[test['user_id'] == val].index[-1])
temp = test['context_timestamp'].diff()
for i in lis:
    temp.loc[i + 1] = 0
temp.loc[0] = 0
test['day_user_diff'] = temp

joint = train[train['is_trade'] == 1000]
joint.drop(['is_trade'], axis=1, inplace=True)
test = pd.concat([test, joint], axis=0)

print('length of test is：', len(test))
new = pd.read_csv(r'D:\Data\IJCAI\test.csv')
print('length of new is：', len(new))
print('is set(new[instance_id].values)==set(test[instance_id].values)',
      set(new['instance_id'].value) == set(test[instance_id].values))
lis = []
for i in new.index:
    lis.extend(list(test[test['instance_id'] == new.loc[i, 'instance_id']].index))
test = test.loc[lis]
test.index = np.arange(len(test))
test.to_csv(r'D:\Data\IJCAI\no_fixed\test_pre1.csv', index=False)