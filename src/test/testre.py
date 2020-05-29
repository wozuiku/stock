import urllib.request
import re

# line = "Cats are smarter than dogs";
#
# matchObj = re.search( r'dogs', line, re.M|re.I)
# if matchObj:
#    print(matchObj.group())
# else:
#    print("No match!!")

url = 'http://money.163.com/special/pinglun/'

result = []

f = urllib.request.urlopen(url)

content = f.read().decode('gbk')
pattern = re.compile(r'<div class="list_item clearfix">.*?</span>',re.S)
basic_content = re.finditer(pattern,content)





for i in basic_content:
    print('basic_content 1')

    print(i.group())

    print('basic_content 2')
    init_dict = {}
    d = re.match(r'<div class="list_item clearfix">.*?<h2><a href="(.*?)">(.*?)</a></h2>.*?<span class="time">(.*?)</span>',i.group(),re.S)
    init_dict['title'] = d.group(2)
    init_dict['created_at'] = d.group(3)
    init_dict['url'] = d.group(1)
    result.append(init_dict)

print (result)
