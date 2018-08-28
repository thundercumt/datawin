#!/usr/bin/env python3

import pymysql

class dao(object):

    def __init__(self):
        self.db = pymysql.connect(host='127.0.0.1',
                                  db='test',
                                  user='miner',
                                  passwd='pass',
                                  port=3306,
                                  charset='utf8mb4')

    def fun(self):
        cursor = self.db.cursor()
        sql = "select * from fc_project_tags"
        try:
            cursor.execute(sql)

            results = cursor.fetchall()
            print("project_id", "tag_name")
            # 遍历结果
            for row in results:
                project_id = row[0]
                tag_name = row[1]
                print(project_id, tag_name)
        except Exception as e:
            raise e
        finally:
            self.db.close()



if __name__ == '__main__':
    db = dao()
    db.fun()