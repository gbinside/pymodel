# coding=utf-8
__author__ = 'roberto gambuzzi (c) 2014'

import MySQLdb as Db
import collections
import copy
from abstract import RecordNotFoundException


class Abstract(object):
    #_tablename = 'prodotti_flat'
    #_chiave = 'sku'
    #_tipo_chiave = 'VARCHAR(255)'

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
            'CREATE TABLE ' + self._tablename + ' (' + self._chiave + ' ' + self._tipo_chiave +
            ' PRIMARY KEY )')
        return self

    def __init__(self, connessione, encoding="iso-8859-1", commit_on_del=True, field_managers=None):
        self._conn = connessione
        self._curs = connessione.cursor()
        self._encoding = encoding
        self._commit_on_del = commit_on_del
        self._data = {}
        self._original_data = None
        if field_managers:
            self._field_managers = field_managers
        if not hasattr(self, '_prefix'):
            self._prefix = None
        if not hasattr(self, '_field_managers'):
            self._field_managers = {}
        try:
            self._execute('SELECT COUNT(*) FROM ' + self._tablename)
        except Db.ProgrammingError:
            self._create_table()
        self.count = 0
        row = self._fetchone()
        if row:
            for x in row:
                self.count = x
        self._execute("show columns from " + self._tablename)
        self._fields = [(x['field'] if type(x) is dict else x[0]) for x in self._fetchall()]

    def set(self, k, v):
        if k not in self._fields:
            self._execute("ALTER TABLE " + self._tablename + " ADD COLUMN " + k + " TEXT;")
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
        if self._original_data == self._data:
            return self
        campi = []
        valori = []
        for k, v in self._data.items():
            campi.append(k)
            if k in self._field_managers:
                helper = self._field_managers[k]
                try:
                    v = helper.dumps(v, encoding=self._encoding)
                except TypeError:
                    v = helper.dumps(v)
                except:
                    print v
                    raise
            valori.append(v)
        interrog = ','.join(['%s'] * len(valori))
        sql = "REPLACE INTO " + self._tablename + " (" + ','.join(campi) + ") VALUES (" + interrog + ")"
        self._execute(sql, valori)
        self._original_data = copy.deepcopy(self._data)
        if commit:
            self._conn.commit()
        return self

    def commit(self):
        self._conn.commit()

    def new(self):
        self._data = {}
        return self

    def load(self, value, field=None):
        if field is None:
            field = self._chiave
        sql = "SELECT * FROM " + self._tablename + " WHERE " + field + " = %s"
        self._execute(sql, (value,))
        ret = self._fetchone()
        if ret:
            if type(ret) is dict:
                self._data = dict(ret)
            else:
                self._data = dict(zip(self._fields, ret))
            for k in self._field_managers:
                helper = self._field_managers[k]
                if k in self._data:
                    try:
                        self._data[k] = helper.loads(self._data[k], object_pairs_hook=collections.OrderedDict)
                    except TypeError:
                        self._data[k] = helper.loads(self._data[k])
        else:
            raise RecordNotFoundException
        self._original_data = copy.deepcopy(self._data)
        return self

    def delete(self):
        sql = "DELETE FROM " + self._tablename + " WHERE " + self._chiave + " = %s"
        self._execute(sql, (self._data[self._chiave],))
        return self

    def __repr__(self):
        return repr(self.__dict__)

    def __del__(self):
        if self._commit_on_del:
            self._conn.commit()

    def collection_keys(self, where_sql=None, vals=None):
        _sql = "SELECT " + self._chiave + " FROM " + self._tablename
        if where_sql:
            _sql = _sql + " WHERE " + where_sql
        self._execute(_sql, vals)
        ret = self._fetchall()
        if ret:
            try:
                return [x[0] for x in ret]
            except KeyError:
                return [x.values()[0] for x in ret]
        else:
            return []

    def get_data(self):
        return self._data

    def set_data(self, **dizio):
        for k, v in dizio.items():
            self.set(k, v)
        return self


def test():
    import os
    import tempfile

    class Prodotto(Abstract):
        _tablename = 'prodotti_flat'
        _chiave = 'sku'
        _tipo_chiave = 'VARCHAR(255)'

    out_conn = Db.connect('localhost', 'root', 'password', 'test')
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

    assert ['123', 'abc'] == self.collection_keys("name LIKE %s", ("% prova%",), )

    p.delete()
    try:
        p = Prodotto(out_conn).load('123')
        ok = True
    except RecordNotFoundException:
        ok = False
    assert not ok


def test2():
    # FIELD MANAGER
    import os
    import tempfile
    import json
    import cPickle

    class Prodotto(Abstract):
        _tablename = 'prodotti_flat'
        _chiave = 'sku'
        _tipo_chiave = 'VARCHAR(255)'
        _field_managers = {'premi': json, 'cast': json, 'raw': cPickle}

    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0].lower()] = row[idx]
        return d

    out_conn = Db.connect('localhost', 'root', 'password', 'test')
    out_conn.text_factory = str
    out_conn.row_factory = dict_factory

    self = Prodotto(out_conn)
    self.set_data(name='prodotto di prova', sku='123',
                  cast={'Attore': ['Brad Pitt', 'Totò'], 'Regista': ['Clint Eastwood']},
                  raw={'Attore': ['Brad Pitt', 'Totò'], 'Regista': ['Clint Eastwood']})
    self.save()

    p = Prodotto(out_conn)
    p.load('123')
    assert p.get('raw') == {'Attore': ['Brad Pitt', 'Totò'], 'Regista': ['Clint Eastwood']}
    assert 'Attore' in p.get('cast')
    assert 'Regista' in p.get('cast')
    try:
        p.get('premi')
        assert False
    except KeyError:
        assert True
    assert p.get('premi', {}) == {}

    p.load('prodotto di prova', 'name')
    assert p.get('raw') == {'Attore': ['Brad Pitt', 'Totò'], 'Regista': ['Clint Eastwood']}
    assert 'Attore' in p.get('cast')
    assert 'Regista' in p.get('cast')
    try:
        p.get('premi')
        assert False
    except KeyError:
        assert True
    assert p.get('premi', {}) == {}


if __name__ == "__main__":
    test()
    test2()