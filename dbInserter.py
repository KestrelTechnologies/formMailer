#!/usr/bin/env python
'''
Project: form Mailer
Author: Spencer Rathbun
Date: 1/19/2014

'''
from sqlalchemy import create_engine, Column, String, types, MetaData, Table, ForeignKey
from sqlalchemy.orm import sessionmaker, mapper, relationship, backref, class_mapper
from sqlalchemy.dialects.mysql.base import CHAR
import uuid, logging, sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import PrimaryKeyConstraint
Base = declarative_base()

# Shared table info
class GUID(types.TypeDecorator):
    """GUID class for use with sqlalchemy and mysql."""
    impl = CHAR
    def __init__(self):
        self.impl.length = 36
        types.TypeDecorator.__init__(self,length=self.impl.length)

    def process_bind_param(self,value,dialect=None):
        if value and isinstance(value,uuid.UUID):
            return value
        elif value and not isinstance(value,uuid.UUID):
            raise ValueError,'value %s is not a valid uuid.UUID' % value
        else:
            return None

    def process_result_value(self,value,dialect=None):
        if value:
            return uuid.UUID(value)
        else:
            return None

    def is_mutable(self):
        return False

def id_column(pk=True):
    """Build an id column for a table."""
    id_column_name = "id"
    return Column(id_column_name,GUID(),primary_key=pk,index=True,default=uuid.uuid4)

def asdict(obj):
    return dict((col.name, getattr(obj, col.name))
            for col in class_mapper(obj.__class__).mapped_table.c)

class Leftovers(object):
    def __init__(self, **entries):
        self.__dict__.update(entries)

class testTable(Base):
    __tablename__ = 'stuff'
    thingy = Column(String(25), primary_key=True, nullable=False)
    typeField = Column(CHAR(3), nullable=False)

class someMoreStuff(Base):
    __tablename__ = 'someMoreStuff'
    __table_args__={'extend_existing':True}
    id = id_column()
    printrec = Column(String(1))
    mailrec  = Column(String(1))
    printed  = Column(String(1), default='N', nullable=False)

    def __init__(self, rec, id=None):
        if id:
            self.id = id
        self.printrec = rec.pop('print?')
        self.mailrec  = rec.pop('mail?')
        self.printed  = "N"

class addresses(Base):
    __tablename__ = 'addresses'
    __table_args__={'extend_existing':True}
    id = id_column()

    someMoreStuff_id = Column(CHAR(36), ForeignKey('someMoreStuff.id'))
    pom = relationship("someMoreStuff", backref=backref("addresses", cascade=""))

    address = Column(String(50),default='',nullable=False, index=True)

    def __init__(self, rec, id=None):
        if id:
            self.id = id
        self.address = rec.pop('address')

# Set up the database connect with sqlalchemy, and build a sessionmaker to create sessions for objects
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
engine = create_engine('sqlite:///:memory:', echo=True)
metadata = MetaData(bind=engine)
sm = sessionmaker(bind=engine)
# for all classes inheriting "Base", check if they exist in the database, and build them if necessary
def buildMetaData():
    Base.metadata.create_all(engine)

class dbInserter(object):
    """This class creates a new session to a sqlite DB, and inserts python dictionaries passed to it, according to the rules herein."""

    def __init__(self, taxtype, county=''):
        self.logger = logging.getLogger('formMailer.dbInserter')
        self.session = sm(autoflush=False, expire_on_commit=False)
        buildMetaData()

    def flush(self):
        """Commit to MySQL DB."""
        buildMetaData()
        self.logger.info('flushing new addresses')
        self.session.flush()

    def commit(self):
        self.flush()
        self.logger.info('Committing to db')
        self.session.commit()

    def rollback(self):
        """Rollback current session."""
        self.session.rollback()

    def get_or_create(self, model, rec):
        """Check if current session has address. If not, query DB for it. If no one has the address, create and flush a new one to the session."""
        instance = self.session.query(model).get((rec['Name'], rec['Address_Line_One'], rec['Address_Line_Two'], rec['Address_Line_Three'], rec['Address_Line_Four']))
        if instance:
            return instance
        else:
            instance = model(rec)
            self.session.add(instance)
            self.session.flush()
            return instance

    def insertRec(self, rec, firstrec=False):
        """Take an input dict, and transform it into the appropriate objs needed."""
        currGUID = uuid.uuid4()

        currAddress = address(rec, id = currGUID)
        self.session.add(currAddress)

