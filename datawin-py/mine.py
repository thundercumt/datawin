#!/usr/bin/env python3

import sys
import itertools
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
        sql = "select * from fc_project_tags limit 1000000 offset 0 "
        try:
            count = cursor.execute(sql)
            ret = []
            results = cursor.fetchall()
            for row in results:
                ret.append(row)
            # print("%d db records occupied %d bytes" % (count, sys.getsizeof(ret),))
            return ret
        except Exception as e:
            raise e
        finally:
            self.db.close()


class AssociationRule(object):

    def __init__(self, rows):
        self.rows = rows

    def process_dataset(self):
        a = dict()
        b = dict()
        for r in self.rows:
            if r[0] not in a:
                a[r[0]] = set();
            a[r[0]].add(r[1])

            if r[1] not in b:
                b[r[1]] = 1
            else:
                b[r[1]] += 1

        return b, {k: tuple(v) for k, v in a.items()}

    def support_set(data, cand_set, kth, min_support):
        d = dict()
        next_cand_set = set()
        next_data = dict()

        print(kth)
        cand_tuples = itertools.combinations(cand_set, kth)
        for c in cand_tuples:
            for k, v in data.items():
                # k is basket, v is purchases

                # optimize when kth is 1, in which case c is single-element set
                if kth == 1:
                    if c[0] in v:
                        if c in d:
                            d[c] += 1
                        else:
                            d[c] = 1

                else:
                    if set(c).issubset(v):
                        if c in d:
                            d[c] += 1
                        else:
                            d[c] = 1

            if c in d and d[c] > min_support:
                next_cand_set = next_cand_set.union(set(c))

        for k, v in data.items():
            if len(v) > kth and len(next_cand_set.intersection(v)) >= kth:
                next_data[k] = v

        return {k: v for k, v in d.items() if v > min_support}, next_cand_set, next_data

    def apriori(self, support=0.05):
        tags, projects = self.process_dataset()
        min_support = int(support * len(projects))

        print('min support', min_support)

        cands = tags.keys()
        next_data = projects
        kth = 1
        while True:
            item_set, cands, next_data = AssociationRule.support_set(next_data, cands, kth, min_support)
            if len(cands) == 0:
                break

            kth += 1
            for k, v in item_set.items():
                print(k, v)


if __name__ == '__main__':
    db = Dao()
    data = db.load_all()
    ar = AssociationRule(data)
    ar.apriori(0.05)
