from sqlalchemy import create_engine, Column, DateTime, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

class MktDBInfo( object ):

    def __init__(self, platform, sqlcore, rootpath):
        self.sqlcore   = sqlcore
        self.rootpath  = rootpath
        self.platform  = platform
        self.engine = create_engine(sqlcore + ':///' + rootpath + 'poloniex.db', echo=True)

    def SqlEngine(self):
        return self.engine

    def SetSqlCore(self, sqlcore):
        self.sqlcore  = sqlcore

    def SetPlatform(slef, platform):
        self.platform = platform

    def SetRootPath(self, rootpath):
        self.rootpath = rootpath

Base = declarative_base()

class PoloMktDB( MktDBInfo ):

    def __init__(self, sqlcore, rootpath):
        MktDBInfo.__init__(self, "Poloniex", sqlcore, rootpath)
        Base.metadata.create_all(self.engine)

    class PoloMktFormat( Base ):

        __tablename__ = "PoloHistoryTrade"
        id = Column(Integer, primary_key=True)
        tradeID   = Column(Integer)
        amount    = Column(Float(1E-8))
        rate      = Column(Float(1E-10))
        date_time = Column(DateTime)
        buy_sell  = Column(Integer)

        def __repr__(self):
            return ("<PoloHistoryTrade>(tradeID='%s', category='%s', national='%s', gender:'%s', year:'%d')" \
            %(self.tradeID, self.amount, self.rate, self.date_time, self.buy_sell))


if __name__ == "__main__":

    polodb = PoloMktDB('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\')
