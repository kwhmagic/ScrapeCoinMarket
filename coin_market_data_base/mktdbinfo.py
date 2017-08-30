import platform
import os
osversion = platform.system()
from sqlalchemy import create_engine, Column, Date, Time, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dataset import connect


def GetClassMap(platformname, pair):

    Base      = declarative_base()

    class PairInfoFormat( Base ):

        __tablename__ = pair + "_" + platformname
        id = Column(Integer, primary_key=True)
        tradeID       = Column(Integer)
        amount        = Column(Float(1E-8))
        rate          = Column(Float(1E-10))
        date          = Column(Date)
        time          = Column(Time)
        buy_sell      = Column(Integer)

        def __repr__(self):
            return ("<%s>(tradeID:'%d', amount:'%.8f', rate:'%.10f', date:'%s', time:'%s', buyOrSell:'%d')\n" \
            %(self.__tablename__, self.tradeID, self.amount, self.rate, self.date, self.time, self.buy_sell))

    return Base, PairInfoFormat


class PairDBInfo( object ):

    def __init__(self, platformname, sqlcore, rootpath, pair):

        split = '\\' if osversion == 'Windows' else '/'
        db_root_path = rootpath + platformname + split

        if not os.path.isdir(db_root_path):
            os.makedirs(db_root_path)

        self._base, self._classmap  = GetClassMap(platformname, pair)
        self._platformname  = platformname
        self._sqlcore       = sqlcore
        self._rootpath      = rootpath
        self._dbpath        = sqlcore + ':///' + db_root_path + pair + '.db'
        self._engine        = create_engine(self._dbpath, echo=False)
        self._base.metadata.create_all(self._engine)
        self._db            = connect(self._dbpath)
        Session = sessionmaker(bind=self._engine)
        self._session = Session()
        self._query   = self._session.query(self._classmap)

class MktDBInfo( object ):

    def __init__(self, platformname, platformobj, sqlcore, rootpath, pairs='all'):

        self._platformobj = platformobj
        self._supportpairs = list(self._platformobj.returnTicker().keys()) if pairs=='all' else pairs
        self._pairsDB = dict()

        for pair in self._supportpairs:
            self._pairsDB[pair] = PairDBInfo(platformname, sqlcore, rootpath, pair)

    def __Add__(self, pair, mktobj):
        db = self._pairsDB.get(pair)
        db._session.add(mktobj)

    def __Add__All__(self, pair, mktobjs):
        db = self._pairsDB.get(pair)
        db._session.add_all(mktobjs)

    def __Delete__(self, pair, delobj):
        db = self._pairsDB.get(pair)
        db._session.query(db._classmap).filter_by(tradeID=delobj.tradeID).delete()

    def __New__(self, pair):
        db = self._pairsDB.get(pair)
        db._session.new

    def __Commit__(self, pair):
        db = self._pairsDB.get(pair)
        db._session.commit()

    def __All__(self, pair):
        db = self._pairsDB.get(pair)
        return db._session.query(db._classmap).all()

    def __Count__(self, pair):
        db = self._pairsDB.get(pair)
        return db._session.query(db._classmap).count()

    def __Last__(self, pair):
        db = self._pairsDB.get(pair)
        return db._session.query(db._classmap).order_by(db._classmap.id.desc()).first()

    def __First__(self, pair):
        db = self._pairsDB.get(pair)
        return db._session.query(db._classmap).first()
