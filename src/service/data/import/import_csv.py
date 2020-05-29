

from src.util.sqlutil import SqlUtil

from datetime import datetime
import csv;
import re;
import requests;
import os




class ImportCsv(object):

    # stock_history新增记录
    def insert_stock_concepts(self, concept_values):

        sqlUtil = SqlUtil()

        sqlUtil.connect();
        cursor = sqlUtil.db.cursor();

        insert_sql = "INSERT INTO stock_concept(concept, code, name)  VALUES " + concept_values + ""

        print('insert_stock_concept insert_sql = ' + insert_sql)

        try:

            cursor.execute(insert_sql);

            sqlUtil.db.commit();
            # print("insert_stock_history sucess");
        except Exception as e:
            # 发生错误时回滚
            sqlUtil.db.rollback();
            print("insert_stock_concept error：" + e);

        # 关闭数据库连接
        sqlUtil.db.close()



    #将csv数据导入数据库
    def import_csv_data(self, data_path, concept):

        importCsv = ImportCsv()

        csv_name = data_path + concept + ".csv";

        stock_concepts_str = ""


        try:

            print('处理文件：' + csv_name)

            with open(csv_name, 'r', encoding='gb2312') as f:
                reader = csv.reader(f)
                next(reader)

                for row in reader:
                    name = row[0]

                    code = ''
                    stock_concept_item = "(" + "\'" + concept + "\'" + "," + "\'" + code + "\'" + "," + "\'" + name + "\'" + ")"

                    stock_concepts_str = stock_concepts_str + stock_concept_item + ","



                print('stock_concepts_str :')
                print(stock_concepts_str)

        except Exception as e:

            print('处理文件：' + csv_name + '出错:' + Exception)

        stock_concepts_str = stock_concepts_str[0: len(stock_concepts_str) - 1]

        importCsv.insert_stock_concepts(stock_concepts_str)




    # 将csv数据导入数据库
    def import_csv_data_batch(self, data_path):

        print('import_csv_data_batch')




    #删除csv数据
    def delete_csv_data(self, data_path):

        files = os.listdir(data_path)

        for file in files:
            file_path = os.path.join(data_path, file)

            print('删除文件：' + file_path)

            os.remove(file_path)





if __name__ == "__main__":

    importCsv = ImportCsv()
    data_path = '/Users/xianxiaoge/PycharmProjects/stock/data/concept/'

    importCsv.import_csv_data_batch(data_path)







