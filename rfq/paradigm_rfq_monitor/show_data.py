import datetime
from time import sleep
import pandas as pd
from dash import Dash, html, dcc,dash_table
from dash.dependencies import Output, Input
import psycopg2
#import time
import threading

from insertdb import adddb
from paradigm import runLog
from dflog import get_logger

def thread1():
    while True:
        
        #print("***Start thread1***")
        try:
            sleep(5)
            adddb()
            sleep(5)   
        except Exception as e:
            log2 = get_logger(name='log',fmt = '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                          file_prefix="info",live_stream=True)
            log2.error(f"writing to database---{str(e)}")
            sleep(3) 
        #print("***End thread1***")

def thread2():
    while True:
        log2 = get_logger(name='log',fmt = '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                          file_prefix="info",live_stream=True)
        log2.info("---Start thread2---")

        try:
            log2.info("Connecting paradigm...") 
            runLog()
  
        except Exception as e:
            log2.error(f"websocket---{str(e)}")
            sleep(3)
            
        log2.info("---End thread2---")
        

def cal_heart_time():
    try:
        with open(r'logs/msg.log', 'r', encoding='utf-8') as f:  # 打开文件
            lines = f.readlines()  # 读取所有行
            last_line = lines[-1]
            s_time = last_line[last_line.rfind("INFO")+7:last_line.rfind(".")] 
            send_time =datetime.datetime.strptime(s_time,"%Y-%m-%d %H:%M:%S")
            now = datetime.datetime.now()
            delta_time = (now - send_time).seconds
    except:
        delta_time = None
    
    return delta_time

""" 
def generate_table(dataframe, max_rows=20):
    #Generate Table 
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])
 """

def sql_query():
    #Set database parameters (Need to connect to the stored database information)
    #conn = psycopg2.connect(host='quant-singapore-postgresql.cluster-ccb2xl9fzp8z.ap-southeast-1.rds.amazonaws.com',port =5432, user='rfq-monitor', passwd='rfq-monitor', db='ParadigmRfqMonitor', charset='utf8')
    conn = psycopg2.connect("dbname='ParadigmRfqMonitor'  user='rfq-monitor' password='rfq-monitor' host='quant-singapore-postgresql.cluster-ccb2xl9fzp8z.ap-southeast-1.rds.amazonaws.com' port='5432'")
    cursor = conn.cursor()
    sql = "SELECT * FROM rfq_data"
    #sql = "SELECT * FROM test"
    cursor.execute(sql)
    results = cursor.fetchall()
    columnDes = cursor.description #Obtain the description information of the connection object
    cursor.close()
    conn.close()
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]
    df = pd.DataFrame([list(i) for i in results],columns=columnNames)
    """ 
    for j in range(df.index[0],df.index[-1],1):
        legs = df.iloc[j,6]      
        if legs.rfind("\n") != -1:
            id = df.iloc[j,0]
            create_time = df.iloc[j,1]
            last_update_time = df.iloc[j,2]
            state = df.iloc[j,3]
            close_reason = df.iloc[j,4]
            description = df.iloc[j,5]
            legs = df.iloc[j,6]
            qty = df.iloc[j,7]
            b_price = df.iloc[j,8]
            d_price = df.iloc[j,9]
            iv = df.iloc[j,10]
            delta = df.iloc[j,11]
            gamma = df.iloc[j,12]
            vega = df.iloc[j,13]
            theta = df.iloc[j,14]
            forward_price = df.iloc[j,15]
            side = df.iloc[j,16]

            count = str(legs).count('\n')
            side1 = str(side).split("\n")
            b_price1 = str(b_price).split("\n")
            d_price1 = str(d_price).split("\n")
            iv1= str(iv).split("\n")
            delta1 = str(delta).split("\n")
            gamma1 = str(gamma).split("\n")
            vega1 = str(vega).split("\n")
            theta1 = str(theta).split("\n")
            forward_price1 = str(forward_price).split("\n")
            forward_price = forward_price1[1]

            for k in range(0,count+1,1):
                if side1[k] == "BUY":
                    b_price1[k] = float(b_price1[k])
                    d_price1[k] = float(d_price1[k])
                    iv1[k] = float(iv1[k])
                    delta1[k] = float(delta1[k])
                    gamma1[k] = float(gamma1[k])
                    vega1[k] = float(vega1[k])
                    theta1[k] = float(theta1[k])          
                elif side1[k] == "SELL":
                    b_price1[k] = - float(b_price1[k])
                    d_price1[k] = - float(d_price1[k])
                    iv1[k] = - float(iv1[k])
                    delta1[k] = - float(delta1[k])
                    gamma1[k] = - float(gamma1[k])
                    vega1[k] = - float(vega1[k])
                    theta1[k] = - float(theta1[k])
                else:
                    with open('info.log','a+') as f:
                        time = datetime.datetime.now()
                        f.write(f"{time}|ERROR|Front end dataframe calculation error")

            b_price = round(sum(b_price1),5)
            d_price = round(sum(d_price1),2)
            iv = round(sum(iv1),4)
            delta = round(sum(delta1)*float(qty),0)
            gamma = round(sum(gamma1)*float(qty),0)
            vega = round(sum(vega1)*float(qty),0)
            theta = round(sum(theta1)*float(qty),0)

            df.loc[f"{j}.1"] = [id,create_time,last_update_time,state,close_reason,description,description,qty,b_price,d_price,
                             iv,delta,gamma,vega,theta,forward_price,side] """     
    
    df.drop_duplicates(subset=['Product_Id'], keep='last', inplace=True)#filter
    try:
        now = datetime.datetime.now() 
        time_m = now -  datetime.timedelta(hours=24)
        df['Create_Time'] = pd.to_datetime(df['Create_Time'])
        df['Last_Update_Time'] = pd.to_datetime(df['Last_Update_Time'])
        #df['Create_Time'].astype('datetime64[ns]')
        df = df[df["Create_Time"] >= time_m]
    except Exception as e:
        log2 = get_logger(name='log',fmt = '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                          file_prefix="info",live_stream=True)
        log2.error(f"dataframe time conversion:{str(e)}")

    
    df.reset_index(drop=True, inplace=True)
    df.drop(columns=['id','Product_Id'], inplace=True)
    results = pd.DataFrame(df,columns=columnNames[1:-1])
    return results
 
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = Dash(__name__, external_stylesheets=external_stylesheets)

t = cal_heart_time()

app.layout = html.Div(
    children=[
        html.Span(
            id='hearttime'
        ),
        html.Div(id='output'),
        dcc.Interval(
        id='interval-component',
        interval = 3*1000,
        n_intervals=0)
    ])

#real-time
@app.callback(Output('hearttime', 'children'),
              Input('interval-component', 'n_intervals'))    
def live_time(t):
    t = cal_heart_time()
    return f'Seconds since update: {t}'

@app.callback(Output('output', 'children'),
              Input('interval-component', 'n_intervals'))
def update_Table(df):
    df = sql_query()
    cols=[{'name': i, "id": i}for i in df.columns]
    cols[5]["name"] = "%Mark_price"
    cols[6]["name"] = "$Mark_price"
    cols[9]["name"] = "$Delta"
    cols[10]["name"] = "$Gamma"
    cols[11]["name"] = "$Vega"
    cols[12]["name"] = "Opt"
    cols[13]["name"] = "$Theta"
    #df = df.sort_values(["id"],ascending=False)
    df = df.iloc[::-1]
    # editable = True,
    table =  dash_table.DataTable(
        id='table',
        columns=cols, 
        style_cell={
            'height': 'auto',
            'whiteSpace': 'pre-wrap',
            'textAlign': 'left',
            'width' :'auto',
            'font_size': '10px',
            'text_align': 'center',
            "editable": 'True'
        },
        data=df.to_dict('records'),
        style_data_conditional=[
        {
            'if': {
                'filter_query': '{State} = "OPEN"',
                'column_id': 'State',
            },
            'backgroundColor': 'white',
            'color': 'black'
        },{
            'if': {
                'filter_query': '{State} = "OPEN"',
                'column_id': 'Close_Reason',
            },
            'backgroundColor': 'white',
            'color': 'black'
        },{
            'if': {
                'filter_query': '{Close_Reason} = "CANCELED_BY_CREATOR"',
                'column_id': 'Close_Reason'
            },
            'backgroundColor': 'rgb(220, 220, 220)',
            'color': 'black'
        },{
            'if': {
                'filter_query': '{Close_Reason} = "CANCELED_BY_CREATOR"',
                'column_id': 'State'
            },
            'backgroundColor': 'rgb(220, 220, 220)',
            'color': 'black'
        },{
            'if': {
                'filter_query': '{Close_Reason} = "EXPIRED"',
                'column_id': 'Close_Reason'
            },
            'backgroundColor': 'rgb(220, 220, 220)',
            'color': 'black'
        },{
            'if': {
                'filter_query': '{Close_Reason} = "EXPIRED"',
                'column_id': 'State'
            },
            'backgroundColor': 'rgb(220, 220, 220)',
            'color': 'black'
        },{
            'if': {
                'filter_query': '{Close_Reason} = "TRADED"',
                'column_id': 'Close_Reason'
            },
            'backgroundColor': '#77FFCC',
            'color': 'black'
        },{
            'if': {
                'filter_query': '{Close_Reason} = "TRADED"',
                'column_id': 'State'
            },
            'backgroundColor': '#77FFCC',
            'color': 'black'
        }],
        )
    return table


if __name__ == '__main__': 
     
    t2 = threading.Thread(target=thread2,name="fun_thread2",daemon=True)
    t2.start()
    
    t1 = threading.Thread(target=thread1,name="fun_thread1",daemon=True) 
    t1.start() 
    
    app.run(debug=True, host="0.0.0.0", port='8050')

