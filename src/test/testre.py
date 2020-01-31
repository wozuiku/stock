import re;

# line = '= "000001"'
#
# matchObj = re.match( r'[0-9]+', line, re.M|re.I)
#
# if matchObj:
#    print ("matchObj.group() : ", matchObj.group())
#    print ("matchObj.group(1) : ", matchObj.group(1))
# else:
#    print ("No match!!")
#
# print(re.search('[0-9]+]', '= "000001"'))
#
# pattern = re.compile(r'[0-9]+')
# print(pattern.search('= "000001"'))

matchObj = re.search( r'[0-9]+', '\'000001', re.M|re.I)
if matchObj:
   print ("search --> matchObj.group() : ", matchObj.group())
else:
   print ("No match!!")