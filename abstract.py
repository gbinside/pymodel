# coding=utf-8
__author__ = 'roberto gambuzzi (c) 2013'

import sqlite3 as sqlite
import collections


class RecordNotFoundException(Exception):
    pass


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
        if field_managers:
            self._field_managers = field_managers
        if not hasattr(self, '_field_managers'):
            self._field_managers = {}
        try:
            self._execute('SELECT COUNT(*) FROM ' + self._tablename)
        except sqlite.OperationalError:
            self._create_table()
        self.count = 0
        row = self._fetchone()
        if row:
            for x in row:
                self.count = x
        self._execute("PRAGMA table_info(" + self._tablename + ")")
        self._fields = [(x['name'] if type(x) is dict else x[1]) for x in self._fetchall()]

    def set(self, k, v):
        if k not in self._fields:
            self._execute("ALTER TABLE " + self._tablename + " ADD COLUMN " + k)
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
        interrog = ','.join(['?'] * len(valori))
        sql = "INSERT OR REPLACE INTO " + self._tablename + " (" + ','.join(campi) + ") VALUES (" + interrog + ")"
        self._execute(sql, valori)
        if commit:
            self._conn.commit()
        return self

    def new(self):
        self._data = {}
        return self

    def load(self, value, field=None):
        if field is None:
            field = self._chiave
        sql = "SELECT * FROM " + self._tablename + " WHERE " + field + " = ?"
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
        return self

    def delete(self):
        sql = "DELETE FROM " + self._tablename + " WHERE " + self._chiave + " = ?"
        self._execute(sql, (self._data[self._chiave],))
        return self

    def __repr__(self):
        return repr(self.__dict__)

    def __del__(self):
        if self._commit_on_del:
            self._conn.commit()

    def collection_keys(self, sql=None, vals=None):
        _sql = "SELECT " + self._chiave + " FROM " + self._tablename
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

    def getData(self):
        return self._data


def test():
    import os
    import tempfile

    class Prodotto(Abstract):
        _tablename = 'prodotti_flat'
        _chiave = 'sku'
        _tipo_chiave = 'VARCHAR(255)'

    handle, db_filename = tempfile.mkstemp()  # "/tmp/test.sqlite"
    print db_filename
    os.close(handle)

    os.remove(db_filename)
    out_conn = sqlite.connect(db_filename, detect_types=sqlite.PARSE_DECLTYPES)
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

    assert ['123', 'abc'] == self.collection_keys("name LIKE ?", ("% prova%",), )

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

    handle, db_filename = tempfile.mkstemp()
    print db_filename
    os.close(handle)

    os.remove(db_filename)
    out_conn = sqlite.connect(db_filename, detect_types=sqlite.PARSE_DECLTYPES)
    out_conn.text_factory = str

    self = Prodotto(out_conn)
    self.set('name', 'prodotto di prova')
    self.set('sku', '123')
    self.set('cast', {'Attore': ['Brad Pitt', 'Totò'], 'Regista': ['Clint Eastwood']})
    self.set('raw', {'Attore': ['Brad Pitt', 'Totò'], 'Regista': ['Clint Eastwood']})
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


if __name__ == "__main__":
    test()
    test2()