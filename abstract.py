__author__ = 'roberto'

import sqlite3 as sqlite
import json
import collections


class RecordNotFoundException(Exception):
    pass


class Abstract(object):
    #tablename = 'prodotti_flat'
    #chiave = 'sku'
    #tipo_chiave = 'VARCHAR(255)'

    def _execute(self, query, vals=None):
        if vals:
            self._curs.execute(query, vals)
        else:
            self._curs.execute(query)
        return self

    def _fetchone(self):
        return self._curs.fetchone()

    def _fetchall(self):
        return self._curs.fetchall()

    def _create_table(self):
        self._execute(
            'CREATE TABLE ' + self.tablename + ' (' + self.chiave + ' ' + self.tipo_chiave +
            ' PRIMARY KEY )')
        return self

    def __init__(self, connessione, commit_on_del=True, json_fields=[]):
        self._conn = connessione
        self._curs = connessione.cursor()
        self._encoding = "iso-8859-1"
        self._commit_on_del = commit_on_del
        self._data = {}
        self._json_fields = json_fields
        try:
            self._execute('SELECT COUNT(*) FROM ' + self.tablename)
        except sqlite.OperationalError:
            self._create_table()
        self.count = 0
        row = self._fetchone()
        if row:
            for x in row:
                self.count = x
        self._execute("PRAGMA table_info(" + self.tablename + ")")
        self._fields = [(x['name'] if type(x) is dict else x[1]) for x in self._fetchall()]

    def set(self, k, v):
        if k not in self._fields:
            self._execute("ALTER TABLE " + self.tablename + " ADD COLUMN " + k)
            self._fields.append(k)
        self._data[k] = v
        return self

    def get(self, k, default=None):
        if default is not None:
            ret = self._data.get(k, default)
        else:
            ret = self._data[k]
        return ret

    def save(self, commit=True):
        campi = []
        valori = []
        for k, v in self._data.items():
            campi.append(k)
            if k in self._json_fields:
                if v is not None:
                    try:
                        v = json.dumps(v, encoding=self._encoding)
                    except:
                        print v
                        raise
                else:
                    v = json.dumps({})
            valori.append(v)
        interrog = ','.join(['?'] * len(valori))
        sql = "INSERT OR REPLACE INTO " + self.tablename + " (" + ','.join(campi) + ") VALUES (" + interrog + ")"
        self._execute(sql, valori)
        if commit:
            self._conn.commit()
        return self

    def new(self):
        self._data = {}
        return self

    def load(self, sku):
        sql = "SELECT * FROM " + self.tablename + " WHERE " + self.chiave + " = ?"
        self._execute(sql, (sku,))
        ret = self._fetchone()
        if ret:
            if type(ret) is dict:
                self._data = dict(ret)
            else:
                self._data = dict(zip(self._fields, ret))
            for k in self._json_fields:
                if k in self._data and self._data[k]:
                    self._data[k] = json.loads(self._data[k], object_pairs_hook=collections.OrderedDict)
        else:
            raise RecordNotFoundException
        return self

    def delete(self):
        sql = "DELETE FROM " + self.tablename + " WHERE " + self.chiave + " = ?"
        self._execute(sql, (self._data[self.chiave],))
        return self

    def __repr__(self):
        return repr(self.__dict__)

    def __del__(self):
        if self._commit_on_del:
            self._conn.commit()

    def collection_keys(self, sql=None, vals=None):
        _sql = "SELECT " + self.chiave + " FROM " + self.tablename
        if sql:
            _sql = _sql + " WHERE " + sql
        self._execute(_sql, vals)
        ret = self._fetchall()
        if ret:
            try:
                return [x[0] for x in ret]
            except KeyError:
                return [x.values()[0] for x in ret]
        else:
            return []


def test():
    import os

    class Prodotto(Abstract):
        tablename = 'prodotti_flat'
        chiave = 'sku'
        tipo_chiave = 'VARCHAR(255)'

    try:
        os.remove("/tmp/test.sqlite")
    except:
        pass
    out_conn = sqlite.connect("test.sqlite", detect_types=sqlite.PARSE_DECLTYPES)
    out_conn.text_factory = str

    self = Prodotto(out_conn)
    self.set('name', 'self di prova')
    self.set('sku', '123')
    self.save()

    try:
        self.get('qty')
        ok = True
    except KeyError:
        ok = False
    assert not ok

    p = Prodotto(out_conn).load('123')
    assert p.get('name') == self.get('name')

    self.new()
    self.set('name', 'self di prova 2')
    self.set('sku', 'abc')
    self.save()

    print self.collection_keys("name LIKE ?", ("% prova%",), )

    p.delete()
    try:
        p = self(out_conn).load('123')
        ok = True
    except RecordNotFoundException:
        ok = False
    assert not ok


if __name__ == "__main__":
    test()