from pymongo import MongoClient
import json
from sklearn.externals import joblib
import pandas as pd
import operator
import os

thisPath = os.getcwd()
thisPath += '/python_src/'

def gu_data():
    conn = MongoClient('113.198.137.114:13306')
    apt = conn.CS2.apt

    return_gu_data={}
    gu_list = ['상당구', '서원구', '흥덕구','청원구']

    for n in range(0,len(gu_list)):
       # gu_data = {}
        term_trade_data = {}
        r = apt.find({"구": gu_list[n]}).sort("계약년도")
        print(gu_list[n]+"시작")
        for i in r:
            if term_trade_data.get(i["계약년도"]) == None:
                term = {}
                term["매매가"] = int(i["거래금액(만원)"].replace(",", ""))
                term["매매량"] = 0
                term_trade_data[i["계약년도"]] = term
            else:
                term = {}
                term["매매가"] = term_trade_data[i["계약년도"]]["매매가"] + int(i["거래금액(만원)"].replace(",", ""))
                term["매매량"] = term_trade_data[i["계약년도"]]["매매량"] + 1
                term_trade_data[i["계약년도"]] = term

        for date in term_trade_data.keys():
            term_trade_data[date]["매매가"] = int(term_trade_data[date]["매매가"] / term_trade_data[date]["매매량"])

            if(term_trade_data.get('2019') == None):
                temp = {}
                temp["매매가"] = 0
                temp["매매량"] = 0
                term_trade_data["2019"] = temp
        print("디비로드 완료")
        print("분석시작")
        X = pd.read_excel("C:/buildTest/my-project/python_src/data/model_data.xlsx")
        X = X.fillna(0)
        df = X.loc[(X["구"] == gu_list[n]), :]
        df.pop("도")
        df.pop("시")
        df.pop("구")
        df.pop("동")
        df.pop("번지")
        df.pop("단지명")
        df.pop("계약일")
        df.pop("도로명")
        df.pop("건설회사")
        df.pop("난방방식")
        df.pop("학군(1)")
        df.pop("거래금액(만원)")
        term = df.copy(deep=False)
        term["거래금액(만원)"] = X.pop("거래금액(만원)")
        term.index = [i for i in range(0, len(df))]
        random_forest_regressor_model = joblib.load('model/'+gu_list[n]+'_random_forest_regressor_model.pkl')
        pred_random = random_forest_regressor_model.predict(df)
        predict_random_price = {}
        date = {}
        for i in range(0, len(df)):
            if predict_random_price.get(str(term["계약년도"][i])) == None:
                predict_random_price[str(term["계약년도"][i])] = pred_random[i]
                date[str(term["계약년도"][i])] = 1
            else:
                predict_random_price[str(term["계약년도"][i])] += pred_random[i]
                date[str(term["계약년도"][i])] += 1
        print("분석끝")

        for i in predict_random_price.keys():
            predict_random_price[i] = int(predict_random_price[i] / date[i])

        date_list = ["2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"]

        for d in date_list:
            if term_trade_data.get(d) == None:
                temp = {}
                temp["매매가"] = 0
                temp["매매량"] = 0
                term_trade_data[d] = temp
            if predict_random_price.get(d) == None:
                predict_random_price[d] = 0

        predict_random_price = dict(sorted(predict_random_price.items(), key=operator.itemgetter(0)))

        trade_price = []
        trade_count = []
        for i in term_trade_data:
            trade_price.append(term_trade_data[i]["매매가"])
            trade_count.append(term_trade_data[i]["매매량"])

        pred_price = []

        for i in term_trade_data:
            pred_price.append(predict_random_price[i])
        print(gu_list[n]+"분석완료")
        return_gu_data[gu_list[n]] = {'trade': term_trade_data, 'predict': predict_random_price}

    with open("data/gu_data.json", 'w') as f:
        json.dump(return_gu_data, f)

def dong_data(addr):
    conn = MongoClient('113.198.137.114:13306')
    apt = conn.CS2.apt
    addr = addr.split(" ")
    gu_name = addr[2]
    dong_name = addr[3]
    if dong_name[-1] == '면' or dong_name[-1] == '읍':
        dong_name = addr[3]+" "+addr[4]
    return_dong_data = {}
    dong_data = {}
    print("디비로드")
    r = apt.find({"동": dong_name}).sort("계약년도")
    for i in r:
        if dong_data.get(i["계약년도"]) == None:
            term = {}
            term["매매가"] = int(i["거래금액(만원)"].replace(",", ""))
            term["매매량"] = 0
            dong_data[i["계약년도"]] = term
        else:
            term = {}
            term["매매가"] = dong_data[i["계약년도"]]["매매가"] + int(i["거래금액(만원)"].replace(",", ""))
            term["매매량"] = dong_data[i["계약년도"]]["매매량"] + 1
            dong_data[i["계약년도"]] = term

    for date in dong_data.keys():
        if dong_data[date]["매매량"] == 0:
            dong_data[date]["매매가"] = 0
        else: dong_data[date]["매매가"] = int(dong_data[date]["매매가"] / dong_data[date]["매매량"])
    print("디비로드 완료")
    print("분석시작")

    X = pd.read_excel(thisPath+"data/"+gu_name+".xlsx")
    X = X.fillna(0)
    df = X.loc[(X["동"] == dong_name), :]
    df.pop("도")
    df.pop("시")
    df.pop("구")
    df.pop("동")
    df.pop("번지")
    df.pop("단지명")
    df.pop("계약일")
    df.pop("도로명")
    df.pop("건설회사")
    df.pop("난방방식")
    df.pop("학군(1)")
    df.pop("거래금액(만원)")

    term = df.copy(deep=False)
    term["거래금액(만원)"] = X.pop("거래금액(만원)")
    term.index = [i for i in range(0, len(df))]

    random_forest_regressor_model = joblib.load(thisPath+'model/'+gu_name+'_random_forest_regressor_model.pkl')
    pred_random = random_forest_regressor_model.predict(df)
    predict_random_price = {}

    date = {}
    for i in range(0, len(df)):
        if predict_random_price.get(str(term["계약년도"][i])) == None:
            predict_random_price[str(term["계약년도"][i])] = pred_random[i]
            date[str(term["계약년도"][i])] = 1
        else:
            predict_random_price[str(term["계약년도"][i])] += pred_random[i]
            date[str(term["계약년도"][i])] += 1

    for i in predict_random_price.keys():
        predict_random_price[i] = int(predict_random_price[i] / date[i])
    print("분석완료")

    predict_random_price = dict(sorted(predict_random_price.items(), key=operator.itemgetter(0)))

    for d in predict_random_price:
        if dong_data.get(d) == None:
            temp={}
            temp["매매가"] = 0
            temp["매매량"] = 0
            dong_data[d] = temp
        if predict_random_price.get(d) == None:
            predict_random_price[d] = 0
            predict_random_price[d] = 0

    return_dong_data[dong_name] = {'trade': dong_data, 'predict': predict_random_price, 'date':list(dong_data.keys())}
    return return_dong_data


def apt_data(addr):
    conn = MongoClient('113.198.137.114:13306')
    apt = conn.CS2.apt
    addr = addr.split(" ")
    apt_gu = addr[2]
    apt_dong = addr[3]
    if apt_dong[-1] == '면' or apt_dong[-1] == '읍':
        apt_dong = addr[3]+" "+addr[4]
        apt_add_num = addr[5]
    else:
        apt_add_num = addr[4]
    apt_data={}
    apt_trade={}


    r = apt.find({"구": apt_gu,"동":apt_dong,"번지":apt_add_num}).sort("계약년도")
    apt_data["apt_info"] = r[0]
    apt_size={}
    c = False

    for i in r:
        if c == False:
            apt_data["apt_info"] = i
            c = True
        if apt_trade.get(i["계약년도"]) == None:
            term = {}
            term["매매가"] = int(i["거래금액(만원)"].replace(",", ""))
            term["매매량"] = 0
            apt_trade[i["계약년도"]] = term
            if apt_size.get(i["전용면적"]) == None:
                apt_size[i["전용면적"]] = 0
        else:
            term = {}
            term["매매가"] = apt_trade[i["계약년도"]]["매매가"] + int(i["거래금액(만원)"].replace(",", ""))
            term["매매량"] = apt_trade[i["계약년도"]]["매매량"] + 1
            apt_trade[i["계약년도"]] = term
            apt_size[i["전용면적"]] = 0
            if apt_size.get(i["전용면적"]) == None:
                apt_size[i["전용면적"]] = 0
    apt_data["apt_info"]["전용면적"] = list(apt_size.keys())
    for date in apt_trade.keys():
        if apt_trade[date]["매매량"] == 0:
            apt_trade[date]["매매가"] = 0
        else: apt_trade[date]["매매가"] = int(apt_trade[date]["매매가"] / apt_trade[date]["매매량"])
    apt_trade = dict(sorted(apt_trade.items(), key=operator.itemgetter(0)))

    for d in apt_trade.keys():
        if apt_trade.get(d) == None:
            term = {}
            term["매매가"] = 0
            term["매매량"] = 0
            apt_trade[d] = term
    apt_data["trade"] = apt_trade
    return apt_data

def return_price_analysis(addr) :
    print(addr)
    addr = addr.split(" ")
    apt_gu = addr[2]
    apt_dong = addr[3]
    if apt_dong[-1] == '면' or apt_dong[-1] == '읍':
        apt_dong = addr[3]+" "+addr[4]
        apt_add_num = addr[5]
    else:
        apt_add_num = addr[4]

    X = pd.read_excel("C:/buildTest/my-project/python_src/data/"+apt_gu+".xlsx")
    X = X.fillna(0)
    df = X.loc[(X["동"] == apt_dong) & (X["번지"] == apt_add_num), :]
    df.pop("도")
    df.pop("시")
    df.pop("구")
    df.pop("동")
    df.pop("번지")
    df.pop("단지명")
    df.pop("계약일")
    df.pop("도로명")
    df.pop("건설회사")
    df.pop("난방방식")
    df.pop("학군(1)")
    df.pop("거래금액(만원)")
    term = df.copy(deep=False)
    term["거래금액(만원)"] = X.pop("거래금액(만원)")
    term.index = [i for i in range(0,len(df))]
    random_forest_regressor_model = joblib.load(thisPath+'model/'+apt_gu+'_random_forest_regressor_model.pkl')

    pred_random = random_forest_regressor_model.predict(df)

    origin_price = {}

    predict_random_price = {}

    date={}
    for i in range(0,len(df)):
        if origin_price.get(str(term["계약년도"][i])) == None:
            origin_price[str(term["계약년도"][i])] = term["거래금액(만원)"][i]
            predict_random_price[str(term["계약년도"][i])] = pred_random[i]

            date[str(term["계약년도"][i])] = 1
        else:
            origin_price[str(term["계약년도"][i])] += term["거래금액(만원)"][i]
            predict_random_price[str(term["계약년도"][i])] += pred_random[i]

            date[str(term["계약년도"][i])] += 1

    for i in origin_price.keys():
        origin_price[i] = int(origin_price[i]/ date[i])
        predict_random_price[i] = int(predict_random_price[i]/ date[i])

    predict_random_price = dict(sorted(predict_random_price.items(), key=operator.itemgetter(0)))

    print("predict_random_price : \n"+ str(list(predict_random_price.values())))
    return list(predict_random_price.values())

def pearson_data(addr):
    addr = addr.split(" ")
    apt_gu = addr[2]
    apt_dong = addr[3]
    if apt_dong[-1] == '면' or apt_dong[-1] == '읍':
        apt_dong = addr[3]+" "+addr[4]
        apt_add_num = addr[5]
    else:
        apt_add_num = addr[4]

    X = pd.read_excel(thisPath+"data/"+apt_gu+".xlsx")
    X = X.loc[X["동"] == apt_dong]
    X = X.fillna(0)
    X.pop("도"); X.pop("시");X.pop("구"); X.pop("동"); X.pop("번지")
    X.pop("단지명"); X.pop("계약년도"); X.pop("계약일")
    X.pop("도로명"); X.pop("건설회사"); X.pop("난방방식")
    X.pop("학군(1)");

    data=X.corr('pearson')
    for i in range(0,len(data["거래금액(만원)"])):
        data["거래금액(만원)"][i] = round(data["거래금액(만원)"][i],2)
    return dict(data["거래금액(만원)"])

def lately_trade(addr):
    conn = MongoClient('113.198.137.114:13306')
    apt = conn.CS2.apt

    addr = addr.split(" ")
    apt_gu = addr[2]
    apt_dong = addr[3]
    if apt_dong[-1] == '면' or apt_dong[-1] == '읍':
        apt_dong = addr[3]+" "+addr[4]
        apt_add_num = addr[5]
    else:
        apt_add_num = addr[4]

    r = apt.find({"구": apt_gu,"동":apt_dong,"번지":apt_add_num}).sort([("계약년도",-1),("계약 월",-1)])
    cnt = apt.find({"구": apt_gu,"동":apt_dong,"번지":apt_add_num}).sort([("계약년도",-1),("계약 월",-1)]).count()
    return_data = []
    if cnt < 5:
        for i in range(0, cnt):
            term = []
            term.append(r[i]["계약년도"] + "/" + r[i]["계약 월"])
            term.append(r[i]["층"])
            term.append(r[i]["거래금액(만원)"])
            return_data.append(term)
    else:
        for i in range(0, 5):
            term=[]
            term.append(r[i]["계약년도"]+"/"+r[i]["계약 월"])
            term.append(r[i]["층"])
            term.append(r[i]["거래금액(만원)"])
            return_data.append(term)

    return return_data

def all_data():
    conn = MongoClient('113.198.137.114:13306')
    apt = conn.CS2.apt

    return_all_data = {}
    term_trade_data={}
    print("디비로드")
    r = apt.find().sort("계약년도")
    for i in r:
        if term_trade_data.get(i["계약년도"]) == None:
            term = {}
            term["매매가"] = int(i["거래금액(만원)"].replace(",", ""))
            term["매매량"] = 0
            term_trade_data[i["계약년도"]] = term
        else:
            term = {}
            term["매매가"] = term_trade_data[i["계약년도"]]["매매가"] + int(i["거래금액(만원)"].replace(",", ""))
            term["매매량"] = term_trade_data[i["계약년도"]]["매매량"] + 1
            term_trade_data[i["계약년도"]] = term
    print("디비로드 완료")
    for date in term_trade_data.keys():
        term_trade_data[date]["매매가"] = int(term_trade_data[date]["매매가"] / term_trade_data[date]["매매량"])

    print("분석시작")
    X = pd.read_excel("C:/buildTest/my-project/python_src/data/model_data.xlsx")
    X = X.fillna(0)
    X.pop("도")
    X.pop("시")
    X.pop("구")
    X.pop("동")
    X.pop("번지")
    X.pop("단지명")
    X.pop("계약일")
    X.pop("도로명")
    X.pop("건설회사")
    X.pop("난방방식")
    X.pop("학군(1)")

    term = X.copy(deep=False)
    term["거래금액(만원)"] = X.pop("거래금액(만원)")
    term.index = [i for i in range(0, len(X))]

    random_forest_regressor_model = joblib.load('model/청주시_random_forest_regressor_model.pkl')
    pred_random = random_forest_regressor_model.predict(X)
    predict_random_price = {}

    date = {}
    for i in range(0, len(X)):
        if predict_random_price.get(str(term["계약년도"][i])) == None:
            predict_random_price[str(term["계약년도"][i])] = pred_random[i]
            date[str(term["계약년도"][i])] = 1
        else:
            predict_random_price[str(term["계약년도"][i])] += pred_random[i]
            date[str(term["계약년도"][i])] += 1

    for i in predict_random_price.keys():
        predict_random_price[i] = int(predict_random_price[i] / date[i])
    print("분석완료")

    date_list = [ "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019" ]

    for d in date_list:
        if term_trade_data.get(d) == None:
            temp={}
            temp["매매가"] = 0
            temp["매매량"] = 0
            term_trade_data[d] = temp
        if predict_random_price.get(d) == None:
            predict_random_price[d] = 0
    predict_random_price = dict(sorted(predict_random_price.items(), key=operator.itemgetter(0)))
    trade_price = []
    trade_count = []
    for i in term_trade_data:
        trade_price.append(term_trade_data[i]["매매가"])
        trade_count.append(term_trade_data[i]["매매량"])

    pred_price = []

    for i in term_trade_data:
        pred_price.append(predict_random_price[i])
    return_all_data = {'trade_price': trade_price, 'trade_count':trade_count,'pred_price': pred_price}
    with open("data/all_data.json", 'w') as f:
        json.dump(return_all_data, f)

# C:/buildTest/my-project/python_src/data

