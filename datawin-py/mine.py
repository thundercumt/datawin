#!/usr/bin/env python3

import sys
import pymysql

class Dao(object):

    def __init__(self):
        self.db = pymysql.connect(host='127.0.0.1',
                                  db='test',
                                  user='miner',
                                  passwd='pass',
                                  port=3306,
                                  charset='utf8mb4')

    def load_all(self):
        cursor = self.db.cursor()
        sql = "select * from fc_project_tags"
        try:
            count = cursor.execute(sql)
            ret = []
            results = cursor.fetchall()
            for row in results:
                ret.append(row)
            print("occupied %d bytes" % (sys.getsizeof(ret),))
            return ret
        except Exception as e:
            raise e
        finally:
            self.db.close()


class AssociationRule(object):

    def __init__(self, rows):
        self.rows = rows

    def apriori_fn(self):
        pass


if __name__ == '__main__':
    db = Dao()
    data = db.load_all()
    ar = AssociationRule(data)
    ar.apriori_fn()