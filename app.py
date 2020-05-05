# -*- coding: utf-8 -*-
# -- coding: utf-8 --

from flask import Flask, jsonify, request, render_template, redirect
from flask_cors import CORS
import os
import json
import python_src.return_data as dp
import python_src.targetList as targetList


class MyFlask(Flask):
  jinja_options = Flask.jinja_options.copy()
  # jinja_options.update(dict(
  #   block_start_string='{%',
  #   block_end_string='%}',
  #   variable_start_string='((',
  #   variable_end_string='))',
  #   comment_start_string='{#',
  #   comment_end_string='#}',
  # ))


APP_ROOT = os.path.dirname(os.path.abspath(__file__))
print(APP_ROOT)

APP_STATIC = os.path.join(APP_ROOT, 'dist')
APP_STATIC2 = os.path.join(APP_ROOT, 'static')
print(APP_STATIC)

# Flask app을 생성한다
app = MyFlask(__name__, static_folder=APP_STATIC, static_url_path='')
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

@app.route('/')
def index():
  app.static_folder = APP_STATIC
  print('인덱스진입')
  return app.send_static_file('index.html')


@app.route('/api/getAllData', methods=['GET'])
def get_all():
  print('대쉬보드 /api/getAllData')
  with open('python_src/data/all_data.json', 'r', encoding='UTF-8-sig') as fr:
    result = json.load(fr)
    print(result)
    return json.dumps(result, ensure_ascii=False)

@app.route('/api/targetJSON', methods=['GET'])
def get_targetJSON():
  print('get_targetJSON /api/targetJSON')
  with open('python_src/data/markers.json', 'r', encoding='UTF-8-sig') as fr:
    result = json.load(fr)
    print(result)
    return json.dumps(result, ensure_ascii=False)

@app.route('/api/targetList', methods=['GET'])
def get_targetList():
  print('tartgetList /api/tartgetList')
  resultJson = targetList.insert_info()
  print(resultJson)
  return json.dumps(resultJson, ensure_ascii=False)

@app.route('/api/getCurAddrInfo', methods=['GET'])
def getCurAddrInfo():
  print('getCurAddrInfo /api/getCurAddrInfo')
  addr = request.args.get('addr', default='', type=str)
  resultJson = targetList.marker_click(addr)
  print(resultJson)
  return json.dumps(resultJson, ensure_ascii=False)

@app.route('/api/one_gu_data', methods=['GET'])
def getone_gu_data():
  print('one_gu_data /api/one_gu_data')
  addr = request.args.get('addr', default='', type=str)
  resultJson = targetList.one_gu_data(addr)
  print(resultJson)
  return json.dumps(resultJson, ensure_ascii=False)

@app.route('/api/si_data', methods=['GET'])
def getsi_data():
  print('si_data /api/si_data')
  addr = request.args.get('addr', default='', type=str)
  resultJson = targetList.si_data(addr)
  print(resultJson)
  return json.dumps(resultJson, ensure_ascii=False)

@app.route('/api/one_dong_data', methods=['GET'])
def getone_dong_data():
  print('one_dong_data /api/one_dong_data')
  addr = request.args.get('addr', default='', type=str)
  resultJson = targetList.one_dong_data(addr)
  print(resultJson)
  return json.dumps(resultJson, ensure_ascii=False)

@app.route('/api/area_info', methods=['GET'])
def getArea_info():
  print('area_info /apit/area_info')
  addr = request.args.get('addr', default='', type=str)
  resultJson = targetList.area_info(addr)
  print(resultJson)
  return json.dumps(resultJson, ensure_ascii=False)

@app.route('/report')
def my_route():
  print("Report 생성")
  app.static_folder = APP_STATIC2
  name = request.args.get('name', default = '', type = str)
  print("파라미터 타입 : "+str(type(name)))
  print("이름 : " + str(name))
  addr = name.split(" ")
  apt_gu = addr[2]
  apt_dong = addr[3]
  if apt_dong[-1] == '면' or apt_dong[-1] == '읍':
    apt_dong = addr[3] + " " + addr[4]
    apt_add_num = addr[5]
  else:
    apt_add_num = addr[4]
  # name = str(name)
  # name1 = name.split(" ")
  # name2 = name.split(" ")
  dongname = apt_dong
  guname = apt_gu
  apt = dp.apt_data(name)
  price = dp.return_price_analysis(name)
  pear = dp.pearson_data(name)
  # dp.gu_data()
  thisPath = os.getcwd()
  thisPath += '/python_src/'
  with open(thisPath+'data/gu_data.json', 'r', encoding='UTF-8-sig') as fr:
    gudata = json.load(fr)
    json.dumps(gudata, ensure_ascii=False)

  dongdata = dp.dong_data(name)
  mylist=dp.lately_trade(name)
  print('apt : \n' + str(apt))
  print('price : \n' + str(price))
  print('pear : \n' + str(pear))
  print('gudata : \n' + str(gudata))
  print('dongdata : \n' + str(dongdata))
  print('mylist : \n' + str(mylist))
  print('name : \n' + str(name))
  print('guname : \n' + str(guname))
  print('dongname : \n' + str(dongname))
  aptdate = list(apt['trade'].keys())
  print(aptdate)
  print(type(aptdate))

  return render_template('report.html',name=name, apt=apt, price=price, pear=pear, gudata=gudata, dongdata=dongdata, mylist=mylist, dongname=dongname, guname=guname, aptdate=aptdate)

@app.route('/api/setMarkers',methods = ['POST'])
def setMarkers() :
  if request.method == 'POST':
    result = request.get_json(silent=True, cache=False)
    # result = request.form('MarkerJSONList')
    print(result) # result 는 dict형식으로 되어있음.
    with open("python_src/data/markers.json", 'w') as f:
        json.dump(result, f)
    return "true"

@app.route('/api/getMarkers')
def getMarkers() :
  with open('python_src/data/markers.json', 'r', encoding='UTF-8-sig') as fr:
    result = json.load(fr)
    print(result)
    return json.dumps(result, ensure_ascii=False)

if __name__ == '__main__':
  app.run(host='0.0.0.0', debug='True')
  # app.run()
