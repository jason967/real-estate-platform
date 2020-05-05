from pymongo import MongoClient
import json
import operator


def apt_name():
  conn = MongoClient('113.198.137.114:13306')
  apt = conn.CS2.apt
  apt_name = []

  r = apt.find({})
  for i in r:
    addr = i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["번지"] + " " + i["단지명"]
    apt_name.append(addr)

  num = 0
  apt_name = set(apt_name)
  return_apt_name = []
  for i in apt_name:
    temp = {}
    temp["id"] = num
    temp["name"] = i
    num += 1
    return_apt_name.append(temp)

  with open("data/apt_name.json", 'w') as f:
    json.dump(return_apt_name, f)

def marker_click(par_addr):
  durationList = []
  Price = []
  amountList = []
  dangeeInfo = {}
  tranPriceList = {}

  conn = MongoClient('113.198.137.114:13306')
  apt = conn.CS2.apt

  addr = par_addr.split(" ")
  apt_gu = addr[2]
  apt_dong = addr[3]
  if apt_dong[-1] == '면' or apt_dong[-1] == '읍':
    apt_dong = addr[3] + " " + addr[4]
    apt_add_num = addr[5]
  else:
    apt_add_num = addr[4]

  pricelist = {}  # 특정 날짜에 특정 층의 거래금액 [날짜][층]
  cnt = {}  # 특정 날짜 특정 층의 매매량 키 : yyyy-mm_층
  pric = {}  # 특정 날짜에 총 매매가
  temp_durationList = {}  # 특정 날짜의 매매량이 들어감
  r = apt.find({"구": apt_gu, "동": apt_dong, "번지": apt_add_num}).sort([("계약년도", 1), ("계약 월", 1)])
  _size = []
  dangeeInfo["총세대수"] = r[0]["총세대수"] if r[0]["총세대수"] != "" else "-"
  dangeeInfo["난방방식"] = r[0]["난방방식"] if r[0]["난방방식"] != "" else "-"
  dangeeInfo["총동수"] = r[0]["총동수"] if r[0]["총동수"] != "" else "-"
  dangeeInfo["학군"] = r[0]["학군(1)"] if r[0]["학군(1)"] != "" else "-"
  for i in r:
    _size.append(i["전용면적"])
    if temp_durationList.get(i["계약년도"] + "-" + i["계약 월"].zfill(2)) == None:  # 처음본 날
      temp_durationList[i["계약년도"] + "-" + i["계약 월"].zfill(2)] = 1
      pric[i["계약년도"] + "-" + i["계약 월"].zfill(2)] = int(i["거래금액(만원)"].replace(",", ""))
    else:  # 이미 본 날
      temp_durationList[i["계약년도"] + "-" + i["계약 월"].zfill(2)] += 1
      pric[i["계약년도"] + "-" + i["계약 월"].zfill(2)] += int(i["거래금액(만원)"].replace(",", ""))

    if pricelist.get(i["계약년도"] + "-" + i["계약 월"].zfill(2) + "_" + i["층"] + "_" + i["전용면적"]) == None:  # 처음 보는 거
      pricelist[i["계약년도"] + "-" + i["계약 월"].zfill(2) + "_" + i["층"] + "_" + i["전용면적"]] = int(
        i["거래금액(만원)"].replace(",", ""))
      cnt[i["계약년도"] + "-" + i["계약 월"].zfill(2) + "_" + i["층"] + "_" + i["전용면적"]] = 1
    else:
      pricelist[i["계약년도"] + "-" + i["계약 월"].zfill(2) + "_" + i["층"] + "_" + i["전용면적"]] += int(
        i["거래금액(만원)"].replace(",", ""))
      cnt[i["계약년도"] + "-" + i["계약 월"].zfill(2) + "_" + i["층"] + "_" + i["전용면적"]] += 1

  pric = dict(sorted(pric.items(), key=operator.itemgetter(0)))
  temp_durationList = dict(sorted(temp_durationList.items(), key=operator.itemgetter(0)))
  pricelist = dict(sorted(pricelist.items(), key=operator.itemgetter(0)))
  cnt = dict(sorted(cnt.items(), key=operator.itemgetter(0)))
  for i in pric:
    durationList.append(i)
    Price.append(int(pric[i] / temp_durationList[i]))
    amountList.append(temp_durationList[i])

  dangeeInfo["단지명"] = addr[-1]
  dangeeInfo["전용면적"] = list(set(_size))

  temp_tranPriceList = []
  key_list = list(pricelist.keys())

  if len(key_list) < 5:
    for i in range(0, len(key_list)):
      l = key_list[i].split("_")
      temp = {}
      temp["거래년월"] = l[0]
      temp["거래 층"] = l[1]
      temp["전용면적"] = l[2]
      temp["거래금액"] = int(pricelist[key_list[i]] / cnt[key_list[i]])
      temp_tranPriceList.append(temp)
  else:
    for i in range(len(key_list) - 1, len(key_list) - 6, -1):
      l = key_list[i].split("_")
      temp = {}
      temp["거래년월"] = l[0]
      temp["거래 층"] = l[1]
      temp["전용면적"] = l[2]
      temp["거래금액"] = int(pricelist[key_list[i]] / cnt[key_list[i]])
      temp_tranPriceList.append(temp)
  apt_info = {}
  apt_info["durationList"] = durationList
  apt_info["Price"] = Price
  apt_info["amountList"] = amountList
  apt_info["dangeeInfo"] = dangeeInfo
  apt_info["tranPriceList"] = temp_tranPriceList
  return apt_info


def insert_info():
  conn = MongoClient('113.198.137.114:13306')
  apt = conn.CS2.apt

  temp_data = {}
  min_max = {}
  # len_size = apt.find({})
  r = apt.find({}).sort([("시", 1), ("구", 1), ("동", 1), ("번지", 1), ("계약년도", 1), ("계약 월", 1), ("계약일", 1)])
  index = 0
  temp_addr = ""
  first_date = ""
  first_check = False
  before_date = ""
  for i in r:
    temp = {}
    temp["id"] = index
    temp["addr"] = i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["번지"]
    if not first_check:
      first_check = True
      first_date = i["계약년도"] + "-" + i["계약 월"].zfill(2)
      temp_addr = temp['addr']
    elif temp_addr != temp['addr']:
      first_last = {}
      first_last["가장옛날"] = first_date
      first_last["가장최근"] = before_date
      temp_data[temp_addr]["duration"] = first_last

    temp_addr = temp['addr']
    before_date = i["계약년도"] + "-" + i["계약 월"].zfill(2)
    temp["newaddr"] = i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["도로명"]
    temp["name"] = i["단지명"]
    if min_max.get(i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["번지"]) == None:
      t = {}
      t["maxPrice"] = t["minPrice"] = int(i["거래금액(만원)"].replace(",", ""))
      min_max[i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["번지"]] = t
    else:
      min_max[i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["번지"]]["minPrice"] = min(
        min_max[i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["번지"]]["minPrice"],
        int(i["거래금액(만원)"].replace(",", "")))
      min_max[i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["번지"]]["maxPrice"] = max(
        min_max[i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["번지"]]["maxPrice"],
        int(i["거래금액(만원)"].replace(",", "")))
    if temp_data.get(i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["번지"]) == None:
      temp_data[i["도"] + " " + i["시"] + " " + i["구"] + " " + i["동"] + " " + i["번지"]] = temp

  temp_data[temp_addr]["duration"] = first_last
  for i in min_max:
    temp_data[i]["minPrice"] = min_max[i]["minPrice"]
    temp_data[i]["maxPrice"] = min_max[i]["maxPrice"]
  return_data = list(temp_data.values())
  return return_data


def area_info(par_addr):
  conn = MongoClient('113.198.137.114:13306')
  apt = conn.CS2.apt

  addr = par_addr.split(" ")
  apt_gu = addr[2]
  apt_dong = addr[3]
  if apt_dong[-1] == '면' or apt_dong[-1] == '읍':
    apt_dong = addr[3] + " " + addr[4]
    apt_add_num = addr[5]
  else:
    apt_add_num = addr[4]

  r = apt.find({"구": apt_gu, "동": apt_dong, "번지": apt_add_num}).sort(
    [("계약년도", 1), ("계약 월", 1), ("계약일", 1), ("전용면적", 1)])
  lately_data = {}

  areainfo = {}
  term_lately_data = {}
  cnt = {}
  for i in r:
    key = i["전용면적"] + "_" + i["계약년도"] + "-" + i["계약 월"].zfill(2)
    if areainfo.get(key) == None:
      areainfo[key] = int(i["거래금액(만원)"].replace(",", ""))
      cnt[key] = 1
    else:
      areainfo[key] += int(i["거래금액(만원)"].replace(",", ""))
      cnt[key] += 1
    key2 = i["전용면적"] + "_" + i["계약년도"] + "-" + i["계약 월"].zfill(2) + "_" + i["계약일"]

    if term_lately_data.get(key2) == None:
      temp = {}
      temp["거래년월"] = i["계약년도"] + "-" + i["계약 월"].zfill(2)
      temp["거래층"] = i["층"]
      temp["전용면적"] = i["전용면적"]
      temp["거래금액"] = int(i["거래금액(만원)"].replace(",", ""))
      cnt[key2] = 1
      term_lately_data[key2] = temp

  areainfo = dict(sorted(areainfo.items(), key=operator.itemgetter(0)))
  cnt = dict(sorted(cnt.items(), key=operator.itemgetter(0)))
  term_lately_data = dict(sorted(term_lately_data.items(), key=operator.itemgetter(0)))

  areainfo2 = {}
  before_size = ""
  temp_date = []
  first_check = False
  for i in term_lately_data:
    l = i.split("_")
    _size = l[0]
    _date = l[1]
    if not first_check:
      first_check = True
      before_size = _size
    if before_size != _size:
      s = len(temp_date)
      lately_data[before_size] = []
      for d in range(s - 1, s - 6, -1):
        temp = {}
        if d < 0:
          temp["거래년월"] = ""
          temp["거래층"] = ""
          temp["전용면적"] = ""
          temp["거래금액"] = 0
          lately_data[before_size].append(temp)
        else:
          lately_data[before_size].append(term_lately_data[temp_date[d]])

      temp_date.clear()
    temp_date.append(i)
    before_size = _size

    index = _size + "_" + _date
    if areainfo2.get(_size) == None:
      temp = {}
      temp_list1 = []
      temp_list2 = []
      temp_list3 = []
      temp["전용면적"] = _size
      temp_list1.append(_date)
      temp_list2.append(int(areainfo[index] / cnt[index]))
      temp_list3.append(cnt[index])
      temp["거래날짜"] = temp_list1
      temp["거래가격"] = temp_list2
      temp["매매량"] = temp_list3
      areainfo2[_size] = temp
    else:
      if areainfo2[_size]["거래날짜"][-1] == _date:
        continue
      areainfo2[_size]["거래날짜"].append(_date)
      areainfo2[_size]["거래가격"].append(int(areainfo[index] / cnt[index]))
      areainfo2[_size]["매매량"].append(cnt[index])

  s = len(temp_date)
  lately_data[before_size] = []
  for d in range(s - 1, s - 6, -1):
    temp = {}
    if d < 0:
      temp["거래년월"] = ""
      temp["거래층"] = ""
      temp["전용면적"] = ""
      temp["거래금액"] = 0
      lately_data[before_size].append(temp)
    else:
      lately_data[before_size].append(term_lately_data[temp_date[d]])

  for i in lately_data:
    areainfo2[i]["lately_data"] = lately_data[i]

  l = list(areainfo2.values())
  return_area_info = []
  for i in l:
    return_area_info.append(i)
  return return_area_info


def si_data(par_addr):
  conn = MongoClient('113.198.137.114:13306')
  apt = conn.CS2.apt
  addr = par_addr.split(" ")
  r = apt.find({"시": addr[1]}).sort([("계약년도", 1), ("계약 월", 1), ("계약일", 1)])
  term_trade_data = {}
  for i in r:
    index = i["계약년도"] + "-" + i["계약 월"].zfill(2)
    if term_trade_data.get(index) == None:
      term = {}
      term["매매가"] = int(i["거래금액(만원)"].replace(",", ""))
      term["매매량"] = 1
      term_trade_data[index] = term
    else:
      term_trade_data[index]["매매가"] += int(i["거래금액(만원)"].replace(",", ""))
      term_trade_data[index]["매매량"] += 1

  term_trade_data = dict(sorted(term_trade_data.items(), key=operator.itemgetter(0)))

  cnt = []
  price = []
  date = []
  for i in term_trade_data:
    cnt.append(term_trade_data[i]["매매량"])
    price.append(int(term_trade_data[i]["매매가"] / term_trade_data[i]["매매량"]))
    date.append(i)

  return_data = {}
  return_data["가장옛날"] = list(term_trade_data.keys())[0]
  return_data["가장최근"] = list(term_trade_data.keys())[-1]
  return_data["매매량"] = cnt
  return_data["매매가"] = price
  return_data["기간"] = list(term_trade_data.keys())
  return return_data


def gu_all_data(par_addr):
  conn = MongoClient('113.198.137.114:13306')
  apt = conn.CS2.apt
  addr = par_addr.split(" ")
  r = apt.find({"시": addr[1]}).sort([("계약년도", 1), ("계약 월", 1), ("계약일", 1)])
  term_trade_data = {}
  for i in r:
    index = i["구"] + "_" + i["계약년도"] + "-" + i["계약 월"].zfill(2)
    if term_trade_data.get(index) == None:
      term = {}
      term["매매가"] = int(i["거래금액(만원)"].replace(",", ""))
      term["매매량"] = 1
      term_trade_data[index] = term
    else:
      term_trade_data[index]["매매가"] += int(i["거래금액(만원)"].replace(",", ""))
      term_trade_data[index]["매매량"] += 1

  term_trade_data = dict(sorted(term_trade_data.items(), key=operator.itemgetter(0)))
  term_data = {}
  first_date = ""
  first_check = False
  before_gu = ""
  before_date = ""
  for i in term_trade_data:
    index = i.split("_")
    if not first_check:
      first_date = index[1]
      first_check = True
      before_gu = index[0]
    if before_gu != index[0]:
      term_data[before_gu]["가장옛날"] = first_date
      term_data[before_gu]["가장최근"] = before_date
    before_gu = index[0]
    before_date = index[1]

    if term_data.get(index[0]) == None:
      cnt = []
      price = []
      date = []
      cnt.append(term_trade_data[i]["매매량"])
      price.append(int(term_trade_data[i]["매매가"] / term_trade_data[i]["매매량"]))
      date.append(index[1])
      temp = {}
      temp["구이름"] = addr[0] + " " + addr[1] + " " + addr[2]
      temp["매매량"] = cnt
      temp["매매가"] = price
      temp["기간"] = date
      term_data[index[0]] = temp
    else:
      term_data[index[0]]["매매량"].append(term_trade_data[i]["매매량"])
      term_data[index[0]]["매매가"].append(int(term_trade_data[i]["매매가"] / term_trade_data[i]["매매량"]))
      term_data[index[0]]["기간"].append(index[1])
  return_data = []
  for i in term_data:
    return_data.append(term_data[i])
  return return_data


def one_gu_data(par_addr):
  conn = MongoClient('113.198.137.114:13306')
  apt = conn.CS2.apt
  addr = par_addr.split(" ")
  r = apt.find({"시": addr[1], "구": addr[2]}).sort([("계약년도", 1), ("계약 월", 1), ("계약일", 1)])
  term_trade_data = {}
  for i in r:
    index = i["계약년도"] + "-" + i["계약 월"].zfill(2)
    if term_trade_data.get(index) == None:
      term = {}
      term["매매가"] = int(i["거래금액(만원)"].replace(",", ""))
      term["매매량"] = 1
      term_trade_data[index] = term
    else:
      term_trade_data[index]["매매가"] += int(i["거래금액(만원)"].replace(",", ""))
      term_trade_data[index]["매매량"] += 1

  term_trade_data = dict(sorted(term_trade_data.items(), key=operator.itemgetter(0)))

  cnt = []
  price = []
  date = []
  for i in term_trade_data:
    cnt.append(term_trade_data[i]["매매량"])
    price.append(int(term_trade_data[i]["매매가"] / term_trade_data[i]["매매량"]))
    date.append(i)

  return_data = {}
  return_data["구이름"] = addr[0] + " " + addr[1] + " " + addr[2]
  return_data["가장옛날"] = list(term_trade_data.keys())[0]
  return_data["가장최근"] = list(term_trade_data.keys())[-1]
  return_data["매매량"] = cnt
  return_data["매매가"] = price
  return_data["기간"] = list(term_trade_data.keys())
  return return_data

def dong_all_data(par_addr):
  conn = MongoClient('113.198.137.114:13306')
  apt = conn.CS2.apt
  addr = par_addr.split(" ")
  r = apt.find({"시": addr[1], "구": addr[2]}).sort([("계약년도", 1), ("계약 월", 1), ("계약일", 1)])
  term_trade_data = {}
  for i in r:
    index = i["동"] + "_" + i["계약년도"] + "-" + i["계약 월"].zfill(2)
    if term_trade_data.get(index) == None:
      term = {}
      term["매매가"] = int(i["거래금액(만원)"].replace(",", ""))
      term["매매량"] = 1
      term_trade_data[index] = term
    else:
      term_trade_data[index]["매매가"] += int(i["거래금액(만원)"].replace(",", ""))
      term_trade_data[index]["매매량"] += 1

  term_trade_data = dict(sorted(term_trade_data.items(), key=operator.itemgetter(0)))
  term_data = {}
  first_date = ""
  first_check = False
  before_dong = ""
  before_date = ""
  for i in term_trade_data:
    index = i.split("_")
    if not first_check:
      first_date = index[1]
      first_check = True
      before_dong = index[0]
    if before_dong != index[0]:
      term_data[before_dong]["가장옛날"] = first_date
      term_data[before_dong]["가장최근"] = before_date
      before_dong = index[0]
    before_date = index[1]

    if term_data.get(index[0]) == None:
      cnt = []
      price = []
      date = []
      cnt.append(term_trade_data[i]["매매량"])
      price.append(int(term_trade_data[i]["매매가"] / term_trade_data[i]["매매량"]))
      date.append(index[1])
      temp = {}
      temp["동이름"] = addr[0] + " " + addr[1] + " " + addr[2] + " " + index[0]
      temp["매매량"] = cnt
      temp["매매가"] = price
      temp["기간"] = date
      term_data[index[0]] = temp
    else:
      term_data[index[0]]["매매량"].append(term_trade_data[i]["매매량"])
      term_data[index[0]]["매매가"].append(int(term_trade_data[i]["매매가"] / term_trade_data[i]["매매량"]))
      term_data[index[0]]["기간"].append(index[1])
  return_data = []
  for i in term_data:
    return_data.append(term_data[i])
  return return_data


def one_dong_data(par_addr):
  conn = MongoClient('113.198.137.114:13306')
  apt = conn.CS2.apt
  addr = par_addr.split(" ")
  dong_name = addr[3]
  if dong_name[-1] == '면' or dong_name[-1] == '읍':
    dong_name = addr[3] + " " + addr[4]
  r = apt.find({"시": addr[1], "구": addr[2], "동": dong_name}).sort([("계약년도", 1), ("계약 월", 1), ("계약일", 1)])
  term_trade_data = {}
  for i in r:
    index = i["계약년도"] + "-" + i["계약 월"].zfill(2)
    if term_trade_data.get(index) == None:
      term = {}
      term["매매가"] = int(i["거래금액(만원)"].replace(",", ""))
      term["매매량"] = 1
      term_trade_data[index] = term
    else:
      term_trade_data[index]["매매가"] += int(i["거래금액(만원)"].replace(",", ""))
      term_trade_data[index]["매매량"] += 1

  term_trade_data = dict(sorted(term_trade_data.items(), key=operator.itemgetter(0)))

  cnt = []
  price = []
  date = []
  for i in term_trade_data:
    cnt.append(term_trade_data[i]["매매량"])
    price.append(int(term_trade_data[i]["매매가"] / term_trade_data[i]["매매량"]))
    date.append(i)

  return_data = {}
  return_data["동이름"] = addr[0] + " " + addr[1] + " " + addr[2] + " " + addr[3]
  return_data["가장옛날"] = list(term_trade_data.keys())[0]
  return_data["가장최근"] = list(term_trade_data.keys())[-1]
  return_data["매매량"] = cnt
  return_data["매매가"] = price
  return_data["기간"] = list(term_trade_data.keys())
  return return_data
