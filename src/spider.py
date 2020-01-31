
import requests;
from lxml import etree;
import re;

from src.util.sqlutil import SqlUtil;


def get_html(url):

    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"};

    response = requests.get(url, headers = headers);
    response.encoding = 'gb2312';

    print(response.apparent_encoding);

    if str(response.status_code).startswith("2"):
        return response.text;
    else:
        return None;


def get_html_utf8(url):

    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"};

    response = requests.get(url, headers = headers);


    if str(response.status_code).startswith("2"):
        return response.text;
    else:
        return None;


def get_stocks(url):

    html = get_html(url);

    if not html:
        print("Response状态码不是2开头");
        return;


    print(html);

    parser = etree.HTML(html);

    items = parser.xpath("//div[@class='quotebody']/div[@id='quotesearch']/ul/li/a");

    sqlUtil = SqlUtil()


    for item in items:

        name_ori = item.xpath('./text()')[0];
        url = item.xpath('./@href')[0];


        pos_1 = name_ori.index('(');
        pos_2 = name_ori.index(')');
        pos_3 = url.index('com');
        pos_4 = url.index('html');


        keycode = url[pos_3 + 4 : pos_4 - 1];

        url2 =url[0 : pos_3 + 4] + "concept/" + keycode + ".html";


        code = name_ori[pos_1+1 : pos_2];
        name = name_ori[0 : pos_1];
        exchange = url[pos_3 + 4 : pos_3 + 6];

        print(keycode);
        print(code);
        print(name);
        print(url);
        print(url2);
        print(exchange);

        #sqlUtil.insert_stock(code, name, url, exchange, '', '');
        sqlUtil.insert_stock(keycode, code, name, url, url2, exchange, '', '');


def get_stock_market(keycode, url):


    html = get_html_utf8(url);

    if not html:
        print("Response状态码不是2开头");
        return;

    #print(html);

    parser = etree.HTML(html);

    close =  parser.xpath("//span[@id='quote-close-custom']/text()");
    avg =  parser.xpath("//span[@id='quote-avg']/text()");
    volume = parser.xpath("//span[@id='quote-volume-custom']/text()");
    open = parser.xpath("//span[@id='quote-open-custom']/text()");
    pc = parser.xpath("//span[@id='quote-pc']/text()");

    amount = parser.xpath("//span[@id='quote-amount-custom']/text()");
    high = parser.xpath("//span[@id='quote-high-custom']/text()");
    low = parser.xpath("//span[@id='quote-low-custom']/text()");
    raisePrice = parser.xpath("//span[@id='quote-raisePrice-custom']/text()");
    fallPrice = parser.xpath("//span[@id='quote-fallPrice-custom']/text()");




    turnoverRate = parser.xpath("//span[@id='quote-turnoverRate']/text()");
    matchObj = re.match(r'<span.*id=\"quote-turnoverRate-custom\".*?>(.*?)<\/span>', html, re.M | re.I);
    #print("html : ", html)
    #print("matchObj.group(1) : ", matchObj)
    volumeRate = parser.xpath("//span[@id='quote-volumeRate']/text()");
    buyOrder = parser.xpath("//span[@id='quote-buyOrder']/text()");
    sellOrder = parser.xpath("//span[@id='quote-sellOrder']/text()");
    industry = parser.xpath("//span[@id='quote-industry']/text()");
    concept = parser.xpath("//span[@id='quote-concept']/text()");
    PERation = parser.xpath("//span[@id='quote-PERation']/text()");
    staticPERation = parser.xpath("//span[@id='quote-staticPERation']/text()");
    EPS = parser.xpath("//span[@id='quote-EPS']/text()");
    PB = parser.xpath("//span[@id='quote-PB']/text()");



    print(turnoverRate[0])
    print(volumeRate[0])
    print(buyOrder[0])
    print(sellOrder[0])
    print(industry[0])
    print(concept[0])
    print(PERation[0])
    print(staticPERation[0])
    print(EPS[0])
    print(PB[0])

    #sqlUtil = SqlUtil()
    #sqlUtil.insert_stock_market(keycode, industry[0], concept[0]);








if __name__ == "__main__":

    url = "http://quote.eastmoney.com/stock_list.html";
    get_stocks(url);

    sqlUtil = SqlUtil()

    rows = sqlUtil.select_stock_urls()
    for row in rows:
        keycode = row[0];



    #get_stock_market("sz300790", "http://quote.eastmoney.com/concept/sz300790.html");


