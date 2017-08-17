from sqlalchemy import create_engine, Column, DateTime, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dataset import connect


class MktDBInfo( object ):

    _supportpairs  = []

    def __init__(self, platformname, platformobj, sqlcore, rootpath, base):
        self._sqlcore       = sqlcore
        self._rootpath      = rootpath
        self._platformname  = platformname
        self._dbpath        = sqlcore + ':///' + rootpath + 'poloniex.db'
        self._engine        = create_engine(self._dbpath, echo=False)
        self._platform      = platformobj
        self._base          = base
        self._base.metadata.create_all(self._engine)
        self._db            = connect(self._dbpath)
        Session = sessionmaker(bind=self._engine)
        self._session = Session()

    def SetPlatform(self, platformobj):
        self._platform = platformobj

    def SqlEngine(self):
        return self._engine

    def SetSqlCore(self, sqlcore):
        self._sqlcore  = sqlcore

    def SetPlatformName(slef, platformname):
        self._platformname = platformname

    def SetRootPath(self, rootpath):
        self._rootpath = rootpath

    def SetSupportPairs(self, supportpairs):
        self._supportpairs = supportpairs[:]

    def __Add__(self, mktobj):
        self._session.add(mktobj)

    def __Add__All__(self, mktobjs):
        self._session.add_all(mktobjs)

    def __Delete__(self, mktobj, delobj):
        self._session.query(mktobj).filter(mktobj.tradeID==delobj.tradeID).delete()

    def __New__(self):
        self._session.new

    def __Commit__(self):
        self._session.commit()

    def __Count__(self, mktobj):
        return self._session.query(mktobj).count()

    def __All__(self, mktobj):
        return self._session.query(mktobj).all()
