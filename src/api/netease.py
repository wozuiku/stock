
import requests;
import time;
import json;



class NetEase(object):

    def get_now_data(self, stock_codes):

        url = 'http://api.money.126.net/data/feed/'+stock_codes+'money.api'
        resp = requests.get(url)

        stock_data_source = resp.content

        start_len = len('_ntes_quote_callback(')
        total_len = len(resp.content)


        stock_data_source = stock_data_source[start_len : total_len - 2]



        return stock_data_source




    def get_his_data(self, stock_code):

        url = 'http://quotes.money.163.com/service/chddata.html'

        if stock_code >= '600000':
            url = url + '?code=0' + stock_code;

        else:
            url = url + '?code=1' + stock_code;

        resp = requests.get(url)

        with open("data/" + stock_code + ".csv", "wb") as code:
            code.write(resp.content)

        print("url:"+url)

        print("save file :"+"data/" + stock_code + ".csv")


    def get_his_data_1year(self, stock_code):

        url = 'http://quotes.money.163.com/service/chddata.html'

        if stock_code >= '600000':
            url = url + '?code=0' + stock_code + "&start=20190101&end=20191231";

        else:
            url = url + '?code=1' + stock_code + "&start=20190101&end=20191231";

        resp = requests.get(url)

        with open("data_1year/" + stock_code + ".csv", "wb") as code:
            code.write(resp.content)

        print("url:"+url)

        print("file :"+"data_1year/" + stock_code + ".csv")


    def get_today_data(self, stock_code, data_path, today):

        # now = int(time.time())
        # timeStruct = time.localtime(now)
        # today = time.strftime("%Y%m%d", timeStruct)

        #today = '20200103'


        url = 'http://quotes.money.163.com/service/chddata.html'

        if stock_code >= '600000':
            url = url + '?code=0' + stock_code + "&start="+today+"&end="+today;

        else:
            url = url + '?code=1' + stock_code + "&start="+today+"&end="+today;

        resp = requests.get(url)

        with open(data_path + stock_code + ".csv", "wb") as code:
            code.write(resp.content)

        print("url:"+url)
        print("file :"+"data/today/" + stock_code + ".csv")


    def get_today_data_thread(self, thread_name, stock_code, data_path, today):

        # now = int(time.time())
        # timeStruct = time.localtime(now)
        # today = time.strftime("%Y%m%d", timeStruct)

        #today = '20200103'


        url = 'http://quotes.money.163.com/service/chddata.html'

        if stock_code >= '600000':
            url = url + '?code=0' + stock_code + "&start="+today+"&end="+today;

        else:
            url = url + '?code=1' + stock_code + "&start="+today+"&end="+today;

        resp = requests.get(url)

        with open(data_path + stock_code + ".csv", "wb") as code:
            code.write(resp.content)

        print(thread_name + " url:"+url)
        print(thread_name + " file :" + "data/today/" + stock_code + ".csv")





if __name__ == "__main__":

    netEase = NetEase();
    stock_codes = '0600519,1000858,';
    stock_data = netEase.get_now_data(stock_codes)

