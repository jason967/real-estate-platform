from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.externals import joblib

gu_list = ['상당구', '서원구', '흥덕구','청원구']
all_data = pd.DataFrame()

for gu_name in gu_list:
    print(gu_name)
    X = pd.read_excel("./data/model_data.xlsx")
    X = X.fillna(0)
    X = X.loc[(X["구"] == gu_name), :]
    if len(X) != 0:
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
        print(X.columns)
        all_data = pd.concat([all_data, term])
        y = X.pop("거래금액(만원)")
        xtrain,xtest,ytrain,ytest = train_test_split(X,y,test_size=0.3,random_state=50)

        random_forest_regressor_model = RandomForestRegressor(n_estimators=200,random_state=10).fit(X=xtrain, y=ytrain)
        joblib.dump(random_forest_regressor_model, 'model/'+gu_name+'_random_forest_regressor_model.pkl')

        random_forest_score = random_forest_regressor_model.score(X=xtest, y=ytest)

all_y = all_data.pop("거래금액(만원)")
xtrain,xtest,ytrain,ytest = train_test_split(all_data,all_y,test_size=0.3,random_state=50)
random_forest_regressor_model = RandomForestRegressor(n_estimators=200, random_state=10).fit(X=xtrain, y=ytrain)
joblib.dump(random_forest_regressor_model, 'model/청주시_random_forest_regressor_model.pkl')
