
import requests;
from lxml import etree;
import re;

from src.util.sqlutil import SqlUtil;


class Spider(object):

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

    def get_gn(self, html):


        if not html:
            print("Response状态码不是2开头");
            return;

        print(html);

        parser = etree.HTML(html);

        items = parser.xpath("//div[@class='cate_inner']");

        for item in items:
            text = item.xpath('./text()')
            print(text)



        # for item in items:
        #
        #     url = item.xpath('./@href');
        #     gn = item.xpath('./text()');
        #
        #
        #
        #     print(url);
        #     print(gn);








if __name__ == "__main__":

    spider = Spider()

    #print(spider.get_html('http://q.10jqka.com.cn/gn/'))

    content = spider.get_html('http://q.10jqka.com.cn/gn/')

    # spider.get_gn(html)

    #print(content)

    result = []

    pattern = re.compile(r'<div class="cate_items">.*?</div>', re.S)
    basic_content = re.finditer(pattern, content)

    for i in basic_content:
        print('basic_content 1')

        print(i.group())

        print('basic_content 2')

        init_dict = {}

        d = re.match(
            r'<div class="cate_items">.*?<a href="(.*?)".*?>(.*?)</a>',
            i.group(), re.S)

        init_dict['url'] = d.group(1)
        init_dict['name'] = d.group(1)

        result.append(init_dict)

    print(result)



