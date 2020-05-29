
import requests;
import re;


class SpiderUtil(object):

    def get_html(self, url):

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"};

        response = requests.get(url, headers=headers);
        response.encoding = 'gb2312';

        print(response.apparent_encoding);

        if str(response.status_code).startswith("2"):
            return response.text;
        else:
            return None;

    def get_html_utf8(self, url):

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"};

        response = requests.get(url, headers=headers);

        if str(response.status_code).startswith("2"):
            return response.text;
        else:
            return None;


    def get_html_test(self, url):

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"};

        response = requests.get(url, headers=headers);

        if str(response.status_code).startswith("2"):
            return response.text;
        else:
            return None;


    def get_html_new(self, url):
        headers = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding':'gzip, deflate',
            'Accept-Language':'zh-CN,zh;q=0.9,und;q=0.8',
            'Cache-Control':'max-age=0',
            'Connection': 'keep-alive',
            'Cookie':'spversion=20130314; user=MDrPzdChuOc6Ok5vbmU6NTAwOjQ4ODYwMzAwMjo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxLDQwOzIsMSw0MDszLDEsNDA7NSwxLDQwOzgsMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDEsNDA6MjU6Ojo0Nzg2MDMwMDI6MTU4MjEyNjQyNzo6OjE1NTE5MjYyMjA6NjA0ODAwOjA6MTBhNWMzNTBkMzExZjJiNjQwNjFjZjFiOGVlMzZlMzA4OmRlZmF1bHRfNDow; userid=478603002; u_name=%CF%CD%D0%A1%B8%E7; escapename=%25u8d24%25u5c0f%25u54e5; ticket=9b2222e5bebcd34d0be659ca306ff262; __utmc=156575163; historystock=603005%7C*%7C300782%7C*%7C300706%7C*%7C300144%7C*%7C603893; __utma=156575163.80218579.1581947282.1582214417.1582297559.3; __utmz=156575163.1582297559.3.3.utmcsr=10jqka.com.cn|utmccn=(referral)|utmcmd=referral|utmcct=/; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1582213977,1582297541,1582297547,1582297830; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1582363974; v=AoNgc7k02bu2vZVPXDehvpUVFEwu-BdVUY1bA7Vg3otZIq06PcinimFc68LG',
            'Host':'q.10jqka.com.cn',
            'Upgrade-Insecure-Requests':'1',
            'Referer': 'http://q.10jqka.com.cn/gn/detail/code/301558/',
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"}

        response = requests.get(url, headers=headers)

        if str(response.status_code).startswith("2"):
            return response.text
        else:
            return None



if __name__ == "__main__":
    url = 'http://q.10jqka.com.cn/gn/detail/field/264648/order/desc/page/3/ajax/1/code/301558'
    spiderUtil = SpiderUtil()
    html = spiderUtil.get_html_new(url)
    print(html)


