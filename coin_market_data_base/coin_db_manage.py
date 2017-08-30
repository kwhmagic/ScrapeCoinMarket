import os
from poloniex_db import PoloMktDB
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from mktdbinfo import GetClassMap
from pandas import read_sql_table, read_sql_query

class CoinDBManage( PoloMktDB ):

    def __init__(self, sqlcore, rootpath, platformname='all', pairs='all'):

        self._sqlcore, self._rootpath = sqlcore, rootpath
        self._supportplatforms = self.__InitSupportPlatform__() if platformname == 'all' else platformname
        self._supportplatforms = [self._supportplatforms] if type(self._supportplatforms) != type([]) else self._supportplatforms
        self._supportpairs, self._supportpairsdbpath = self.__InitSupportPairsAndDBPath__(self._rootpath, self._supportplatforms)

        self._db_engines, self._db_bases, self._tablenames = dict(), dict(), dict()
        self.__ConnectDB__(self._db_engines, self._db_bases, self._tablenames, self._sqlcore, self._supportpairsdbpath)

    def __InitSupportPlatform__(self):
        return  ["Poloniex"]

    def __InitSupportPairsAndDBPath__(self, rootpath, supportplatforms):

        dbpaths_each_paltform, support_pairs, support_pair_db_paths = [], dict(), dict()
        for platformname in supportplatforms:
            dbpaths_each_paltform.append(rootpath + platformname)
            support_pairs[platformname] = []
            support_pair_db_paths[platformname] = dict()

        for dbpath in dbpaths_each_paltform:
            for file in os.listdir(dbpath):
                if file.endswith(".db"):
                    pair = file.split(".")[0]
                    support_pairs[platformname].append(pair)
                    support_pair_db_paths[platformname][pair] = os.path.join(dbpath, file)

        return support_pairs, support_pair_db_paths

    #dict() : support_pair_db_paths[Platformname]
    def __ConnectDB__(self, db_engines, db_bases, tablenames, sqlcore, support_pair_db_paths):
        for platformname, db_pair_paths in support_pair_db_paths.items():
            db_engines[platformname], db_bases[platformname], tablenames[platformname] = dict(), dict(), dict()
            for pair, db_path in db_pair_paths.items():
                db_engine = create_engine(sqlcore+':///'+db_path)
                db_engines[platformname][pair] = db_engine
                db_bases[platformname][pair]   = declarative_base(db_engine)
                tablenames[platformname][pair], = db_engine.table_names()

    def SupportPairs(self):
        return self._supportpairs

    def __LoadSession__(self, platformname, pair):
        metadata = self._db_bases[platformname][pair].metadata
        Session = sessionmaker(bind=self._db_engines[platformname][pair])
        session = Session()
        return session

    def PairDataFromXPlatform_All(self, pair, platformname='all', convert2dataframe=False):

        support_platform = []
        data_from_diff_platform = dict()

        if platformname == 'all':
            support_platform = self._supportplatforms
        else:
            support_platform = platformname
            support_platform = [support_platform] if type(support_platform) != type([]) else support_platform

        if not convert2dataframe:
            for platformname in support_platform:
                session = self.__LoadSession__(platformname, pair)
                data_from_diff_platform[platformname] = session.query(GetClassMap(platformname, pair)[1]).all()
        else:
            data_from_diff_platform[platformname] = read_sql_query("SELECT * FROM " + self._tablenames[platformname][pair], \
            self._db_engines[platformname][pair], index_col = 'tradeID')

        return data_from_diff_platform
