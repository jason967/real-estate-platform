from urllib.request import Request,urlopen
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import json
def main():
    data_set = []
    for Y in range(11,12):
        for M in range(1,2):
            Month=""
            if M<10:
                Month = '0'+str(M)
            elif M>=10:
                Month=str(M)
            print(str(Y)+Month)
            url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade"
            queryParams = '?LAWD_CD=' + '43111' + '&DEAL_YMD=' + '20'+str(Y)+Month+'&serviceKey=' + 'lPH2FCqmg7Sbrs6Qa17nQQ7LYJ%2B483FqHpS9yR1w%2B2mlMv6Ye5X53o%2Bua9X8lBi13nr1rL7WQaIXB6BaxKsrwg%3D%3D'
            url = url+queryParams
            print(url)
            resultXML = urllib.request.urlopen(url)
            result = resultXML.read()
            xmlsoup = BeautifulSoup(result, "lxml-xml")
            te = xmlsoup.findAll("item")
            for t in te:
                build_y = t.find("건축년도").text
                year = t.find("년").text
                month = t.find("월").text
                day = t.find("일").text
                dong = t.find("법정동").text
                apt_nm = t.find("아파트").text
                size = t.find("전용면적").text
                price = t.find("거래금액").text
                floor = t.find("층").text
                jibun = t.find("지번").text
                data_set.append([dong,jibun, apt_nm, size, year, month, day, price,floor,build_y])
    data = pd.DataFrame(data_set)
    data.columns = ['동','번지','아파트명','전용면적','계약년도','계약 월','계약 일','거래금액','층','건축년도']
    data.to_json('test.json', orient='table')
    json_data = open('./test.json').read()

    tmp = json.loads(json_data)
    print(tmp)
    # data.to_csv('SangdangGu.csv',mode='a',encoding='cp949')
    # data.to_csv('SeowonGu.csv',mode='a',encoding='cp949')
    # data.to_csv('HeungdeokGu.csv',mode='a',encoding='cp949')
    # data.to_csv('CheongwonGu.csv',mode='a',encoding='cp949')
main()

# 43111 : 충청북도 청주시 상당구
# 43112 : 충청북도 청주시 서원구
# 43113 : 충청북도 청주시 흥덕구
# 43114 : 충청북도 청주시 청원구
