# coding:utf-8 
#Connect to database and create table
from sqlalchemy import VARCHAR, create_engine,  Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dflog import get_logger

BaseModel = declarative_base()
log2 = get_logger(name='log',fmt = '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                  file_prefix="info",live_stream=True)
log2.info("Start connecting to database...")

DB_CONNECT = 'postgresql+psycopg2://rfq-monitor:rfq-monitor@quant-singapore-postgresql.cluster-ccb2xl9fzp8z.ap-southeast-1.rds.amazonaws.com:5432/ParadigmRfqMonitor' 
engine = create_engine(DB_CONNECT)
DB_Session = sessionmaker(bind=engine)
session = DB_Session()

log2.info("Successfully connected...")

class Access(BaseModel):
    __tablename__ = 'rfq_data'
    #__tablename__ = 'test'
    id = Column(Integer,primary_key=True,autoincrement=True)
    Role = Column(VARCHAR(10))
    Side = Column(VARCHAR(50))
    Qty = Column(VARCHAR(100))
    Description = Column(VARCHAR(400))
    Legs = Column(VARCHAR(255))
    B_Mark_price = Column(VARCHAR(255))
    D_Mark_price = Column(VARCHAR(255))
    Mark_IV = Column(VARCHAR(255))
    Forward_Price = Column(VARCHAR(255))
    Delta_Dollar = Column(VARCHAR(255))
    Gamma_Dollar = Column(VARCHAR(255))
    Vega_Dollar = Column(VARCHAR(255))
    Option_Count = Column(VARCHAR(255))
    Theta_Dollar = Column(VARCHAR(255)) 
    State = Column(VARCHAR(255))
    Close_Reason = Column(VARCHAR(255))
    Create_Time = Column(VARCHAR(255))
    Last_Update_Time = Column(VARCHAR(255))
    Product_Id = Column(VARCHAR(255))
    
    
    log2.info('Establishing Table')
    def __init__(self,Role,Side,Qty,Description,Legs,B_Mark_price,D_Mark_price,
                 Mark_IV,Forward_Price,Delta_Dollar,Gamma_Dollar,Vega_Dollar,Theta_Dollar,State,Close_Reason,
                 Create_Time,Last_Update_Time,Option_Count,Product_Id):
        self.Role = Role
        self.Side = Side
        self.Qty = Qty
        self.Description = Description
        self.Legs = Legs
        self.B_Mark_price = B_Mark_price
        self.D_Mark_price = D_Mark_price
        self.Mark_IV = Mark_IV
        self.Forward_Price = Forward_Price
        self.Delta_Dollar = Delta_Dollar
        self.Gamma_Dollar = Gamma_Dollar
        self.Vega_Dollar = Vega_Dollar
        self.Theta_Dollar = Theta_Dollar
        self.State = State
        self.Close_Reason = Close_Reason
        self.Create_Time = Create_Time
        self.Last_Update_Time = Last_Update_Time
        self.Option_Count = Option_Count
        self.Product_Id  = Product_Id
        
    
    def check_existing(self):
        existing = session.query(Access).filter_by(Product_Id=self.Product_Id,Create_Time=self.Create_Time,Last_Update_Time=self.Last_Update_Time,Description = self.Description,State = self.State,Close_Reason = self.Close_Reason).first()
        if not existing:
            data = Access(Role=self.Role,Side=self.Side,Qty=self.Qty,Description=self.Description,Legs= self.Legs,
                          B_Mark_price=self.B_Mark_price,D_Mark_price=self.D_Mark_price,
                          Mark_IV=self.Mark_IV,Forward_Price=self.Forward_Price,Delta_Dollar=self.Delta_Dollar,Gamma_Dollar=self.Gamma_Dollar,
                          Vega_Dollar=self.Vega_Dollar,Theta_Dollar=self.Theta_Dollar,State=self.State,Close_Reason=self.Close_Reason,
                          Create_Time=self.Create_Time,Last_Update_Time=self.Last_Update_Time,Option_Count=self.Option_Count,Product_Id=self.Product_Id)
        else:
            data = existing
        return data
    
   
def init_db():
    BaseModel.metadata.create_all(engine)
def drop_db():
    BaseModel.metadata.drop_all(engine) 

#drop_db()
#init_db()  
