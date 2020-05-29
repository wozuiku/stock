
import requests;
import time
import re

from src.util.spiderutil import SpiderUtil;



class Jqka(object):

    def get_concepts(self, url):
        spiderUtil = SpiderUtil()
        html = spiderUtil.get_html(url)

        concept_array = []
        concept_item = []

        pattern_group = re.compile(r'<div class="cate_items">.*?</div>', re.S)
        concept_groups = re.finditer(pattern_group, html)

        for i in concept_groups:
            # print('basic_content 1')
            # print(i.group())
            # print('basic_content 2')

            pattern_item = re.compile(r'<a.*?/a>', re.S)
            concept_items = re.finditer(pattern_item, i.group())

            for j in concept_items:

                # print(j.group())

                stock_item = re.match(
                    r'<a href="(.*?)".*?>(.*?)</a>',
                    j.group(), re.S)

                stock_item2 = re.match(
                    r'<a href=".*?code/(.*?)/".*?>',
                    j.group(), re.S)

                concept_array.append([stock_item.group(1), stock_item.group(2), stock_item2.group(1)])

        # print(concept_array)
        # print(len(concept_array))

        return concept_array



    def get_page_count(self, url):
        spiderUtil = SpiderUtil()
        html = spiderUtil.get_html(url)

        #print(html)

        pattern_page = re.compile(r'<span class="page_info">.*?</span>', re.S)
        page_groups = re.finditer(pattern_page, html)

        page_count = 0

        for i in page_groups:
            # print('basic_content 1')
            # print(i.group())
            # print('basic_content 2')

            page_info = re.match(
                r'<span class="page_info">1/(.*?)</span>',
                i.group(), re.S)

            # print(page_info.group(1))

            # return page_info.group(1)
            page_count = page_info.group(1)

            if page_count is None:
                page_count = 1

            return page_count





    def get_next_page(self, index, code):

        url = 'http://q.10jqka.com.cn/gn/detail/field/264648/order/desc/page/'+index+'/ajax/1/code/'+code

        print(url)

        spiderUtil = SpiderUtil()

        html = spiderUtil.get_html_new(url)

        print(html)

        stock_array = []
        stock_item = []

        pattern_group = re.compile(r'<tbody>.*?</tbody>', re.S)
        stock_groups = re.finditer(pattern_group, html)

        for i in stock_groups:
            print('basic_content 1')
            print(i.group())
            print('basic_content 2')

            pattern_item = re.compile(r'<tr.*?/tr>', re.S)
            stock_items = re.finditer(pattern_item, i.group())

            for j in stock_items:
                print(j.group())

                stock_item = re.match(
                    r'<tr>.*?<a href.*?>(.*?)</a>.*?<a href.*?>(.*?)</a>.*?<td class="c-rise">(.*?)</td>.*?<td>(.*?)</td>',
                    j.group(), re.S)

                print(stock_item.group(1))
                print(stock_item.group(2))
                print(stock_item.group(3))
                print(stock_item.group(4))

                stock_array.append([stock_item.group(1), stock_item.group(2), stock_item.group(3), stock_item.group(4)])

        # print(stock_array)
        # print(len(stock_array))

        return stock_array



if __name__ == "__main__":

    url = 'http://q.10jqka.com.cn/gn'
    jqka = Jqka()
    concept_array = jqka.get_concepts(url)
    print(concept_array)

    for concept in concept_array:
        concept_url = concept[0]
        concept_name = concept[1]
        concept_code = concept[2]

        print(concept_url)
        print(concept_name)
        print(concept_code)

        page_count = jqka.get_page_count(concept_url)

        print(page_count)

        for index in range(1, int(page_count)):
            stock_array = jqka.get_next_page(str(index), concept_code)

            print(stock_array)






