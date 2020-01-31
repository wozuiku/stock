
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from email.header import Header
from src.util.sqlutil import SqlUtil;
import time




class EmailUtil(object):

    def send_email(self, batch):
        sender = '52970496@qq.com'  # 发件人邮箱账号
        passwd = 'wkbhiknpajjmbjie'  # 发件人邮箱密码
        receiver = '52970496@qq.com;'  # 收件人邮箱账号，我这边发送给自己
        #receiver = 'jiangshuqi_hit@163.com'  # 收件人邮箱账号，我这边发送给自己
        #receiver = 'sjsun_whhit@163.com'  # 收件人邮箱账号，我这边发送给自己

        # now = int(time.time())
        # timeStruct = time.localtime(now)
        # batch = time.strftime("%Y%m%d", timeStruct)

        sqlUtil = SqlUtil()

        rows = sqlUtil.select_stock_select(batch)

        lines = "";


        for row in rows:
            industry = row[0];
            code = row[1];
            name = row[2];
            sector = row[3];
            url = row[4];
            remark = row[5];

            line = "<tr><td>"+industry+"</td><td>"+code+"</td><td>"+name+"</td><td>"+sector+"</td><td>"+remark+"</td></tr> "
            lines = lines + line




        try:
            mail_msg = """
            <table border = "1" >
            <tr><th>行业</th><th>代码</th><th>名称</th><th>板块</th><th>备注</th></tr>
            """ + lines + """
            </table >
            """

            msg = MIMEText(mail_msg, 'html', 'utf-8')
            msg['From'] = formataddr(["贤小哥", sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
            msg['To'] = formataddr(["FK", receiver])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
            subject = batch + "股票信息筛选"
            msg['Subject'] = Header(subject, 'utf-8')  # 邮件的主题，也可以说是标题

            server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
            server.login(sender, passwd)  # 括号中对应的是发件人邮箱账号、邮箱密码
            server.sendmail(sender, [receiver, ], msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
            server.quit()  # 关闭连接

            print('邮件发送成功')

        except Exception as e:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
            ret = False
            print('错误明细是', e)
            print('邮件发送失败')


if __name__ == "__main__":
    emailUtil = EmailUtil()
    emailUtil.send_email()





