import datetime
import time
from datasave import Access,session
from dflog import get_logger


def adddb():
    data_all = {}
    log2 = get_logger(name='log',fmt = '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                      file_prefix="info",live_stream=True)
    
    with open(r'logs/data.log',encoding='utf-8') as f:  
        count=-1
        for count, line in enumerate(f):
            count+=1
        if count >= 10:
            m = -10
        else:
            m = 0 
        
    with open(r'logs/data.log',encoding='utf-8') as f:    
        log2.info("Start read the data")  
        count = 1 
        for line in f.readlines()[m:]:
            data_ev = {} 
            newline = f"{line}"       
            #Made a simple division using length         
            if len(line)  > 80 and line[26:30] == 'INFO' and line[35:42] == 'jsonrpc' :   
                try:
                    #created_at_time
                    time3 = newline[newline.rfind("created_at")+13:newline.rfind("'description'")-2]
                    timeStamp1 = float(float(time3)/1000) 
                    timeArray1 = time.localtime(timeStamp1) 
                    time1 =time.strftime("%Y-%m-%d %H:%M:%S", timeArray1) 
                    created_at_time2 = datetime.datetime.strptime(time1,"%Y-%m-%d %H:%M:%S")
                    created_at_time3 = created_at_time2 + datetime.timedelta(hours=8)
                    created_at_time = str(created_at_time3)
                    
                    #last_updated_at_time
                    time4 = newline[newline.rfind("last_updated_at")+18:newline.rfind("legs")-3]
                    timeStamp2 = float(float(time4)/1000) 
                    timeArray2 = time.localtime(timeStamp2) 
                    updated_at_time =time.strftime("%Y-%m-%d %H:%M:%S", timeArray2)
                    last_updated_at_time2 = datetime.datetime.strptime(updated_at_time,"%Y-%m-%d %H:%M:%S")
                    last_updated_at_time3 = last_updated_at_time2 + datetime.timedelta(hours=8)
                    last_updated_at_time = str(last_updated_at_time3)

                    #role
                    role = newline[newline.rfind("role")+8:newline.rfind("side_layering_limit")-4]
                    #description
                    b = newline[newline.rfind("'description'")+16:newline.rfind("'expires_at'")-3]
                    description1 = b.replace("\\n","/ \n")
                    description = description1.replace("  "," ")             
                    #quantity
                    quantity1 = newline[newline.rfind("'product_codes'"):newline.rfind("'quote_currency'")]
                    quantity = quantity1[quantity1.rfind("'quantity'")+13:-3]
                    #state
                    state = newline[newline.rfind('state')+9:newline.rfind('strategy_code')-4]
                    #product_id
                    product_id = newline[newline.rfind("'id'")+7:newline.rfind('is_taker_anonymous')-4]
                    #closed_reason
                    closed_reason1 = newline[newline.rfind('closed_reason')+16:newline.rfind('counterparties')-3]
                    if closed_reason1 == 'None':
                        closed_reason = closed_reason1
                    else:
                        closed_reason = closed_reason1[1:-1]
                        if closed_reason == 'EXECUTION_LIMIT':
                            closed_reason = 'TRADED'

                    n = description.count('/')+1
                    #price
                    pros = newline[newline.rfind('legs'):newline.rfind('product_codes')]
                    if description.rfind("/") != -1:
                        legsl = []
                        sides = []
                        ivs = []
                        b_prices = []
                        d_prices = []
                        deltas = []
                        gammas = []
                        vegas = []
                        thetas = []
                        forward_prices = []
                        option_counts = []

                        b_prices_c = []
                        d_prices_c = []
                        deltas_c=[]
                        gammas_c = [] 
                        vegas_c = []
                        thetas_c = []
                        option_counts_c = []
                        

                        for i in range(1,n+1,1):
                            #leg side
                            p = pros.split("instrument_id")
                            q= p[i]
                            legss = q[q.rfind('instrument_name')+19:q.rfind('price')-4]
                            legsl.append(legss)
                            sidess = q[q.rfind('side')+8:q.rfind("'}")]
                            sides.append(sidess)
                            #forwardprice iv
                            other = newline[newline.rfind("meta"):newline.rfind("strategy")]
                            m = other.split("forward price")
                            m1 = m[i]
                            forward_pricess = round(float(m1[3:m1.rfind("IV")-3]),2)
                            forward_prices.append(str(forward_pricess))
                            ivss = m1[m1.rfind("IV")+5:m1.rfind("delta")-3]
                            ivs.append(ivss)
                            #rep:b_price  d_price  delta  gamma  vega  theta
                            one = newline[newline.rfind("strategy"):]
                            twos = one.split("rep")
                            two = twos[i]          
                            b_pricel = two[two.rfind("forward")+8:two.rfind("priceUSD")-3]        
                            b_pricess = round(float(b_pricel[b_pricel.rfind("price\'")+8:]),6)
                            b_prices.append(str(b_pricess))
                            d_pricess = round(float(two[two.rfind("priceUSD")+11:two.rfind("deltaDollar")-3]),2)
                            d_prices.append(str(d_pricess))

                            deltass = round(float(two[two.rfind("deltaDollar")+14:two.rfind("gammaDollar")-3])*float(quantity),2)
                            deltas.append('$'+str(format(deltass,",")))
                            gammass = round(float(two[two.rfind("gammaDollar")+14:two.rfind("vegaDollar")-3])*float(quantity),2)
                            gammas.append('$'+str(format(gammass,",")))
                            vegass = round(float(two[two.rfind("vegaDollar")+13:two.rfind("thetaDollar")-3])*float(quantity),2)
                            vegas.append('$'+str(format(vegass,",")))
                            thetass = round(float(two[two.rfind("thetaDollar")+13:two.rfind("thetaPercRef")-3])*float(quantity),2)
                            thetas.append('$'+str(format(thetass,",")))
                            #option_count
                            option_countss = round(float(two[two.rfind("optionCount")+14:two.rfind("optionCount")+30])*float(quantity),2)
                            option_counts.append(str(option_countss))

                            if sidess == 'BUY':
                                b_pricess_c = b_pricess
                                d_pricess_c = d_pricess
                                deltass_c = deltass
                                gammass_c = gammass
                                vegass_c = vegass
                                thetass_c = thetass
                                option_countss_c = option_countss
                            else:
                                b_pricess_c = - b_pricess
                                d_pricess_c = - d_pricess
                                deltass_c = - deltass
                                gammass_c = - gammass
                                vegass_c = - vegass
                                thetass_c = - thetass
                                option_countss_c = - option_countss
                            """ 
                            if role == 'MAKER':
                                b_pricess_c = - b_pricess_c1
                                d_pricess_c = - d_pricess_c1
                                deltass_c = - deltass_c1
                                gammass_c = - gammass_c1
                                vegass_c = - vegass_c1
                                thetass_c = - thetass_c1
                                option_countss_c = - option_countss_c1
                            else:
                                b_pricess_c = b_pricess_c1
                                d_pricess_c = d_pricess_c1
                                deltass_c = deltass_c1
                                gammass_c = gammass_c1
                                vegass_c = vegass_c1
                                thetass_c = thetass_c1
                                option_countss_c = option_countss_c1 """
                                                    
                            b_prices_c.append(b_pricess_c)
                            d_prices_c.append(d_pricess_c)
                            deltas_c.append(deltass_c)
                            gammas_c.append(gammass_c)
                            vegas_c.append(vegass_c)   
                            thetas_c.append(thetass_c) 
                            option_counts_c.append(option_countss_c) 

                        b_price_c = round(sum(b_prices_c),5)
                        b_prices.append(str(b_price_c))
                        d_price_c = round(sum(d_prices_c),2)
                        d_prices.append(str(d_price_c))
                        delta_c = round(sum(deltas_c),2)
                        deltas.append('$'+str(format(delta_c,",")))
                        gamma_c = round(sum(gammas_c),2)
                        gammas.append('$'+str(format(gamma_c,",")))
                        vega_c = round(sum(vegas_c),2)
                        vegas.append('$'+str(format(vega_c,",")))
                        theta_c = round(sum(thetas_c),2)
                        thetas.append('$'+str(format(theta_c,",")))
                        option_count_c = round(sum(option_counts_c),2)
                        option_counts.append(str(option_count_c))

                        legsl.append('TOTAL')
                        side = "\n".join(sides)
                        legs = "\n".join(legsl) 
                        iv = "\n".join(ivs)
                        forward_price = forward_prices[0]
                        b_price = "\n".join(b_prices)
                        d_price = "\n".join(d_prices)
                        delta = "\n".join(deltas)
                        gamma = "\n".join(gammas)
                        vega = "\n".join(vegas)
                        theta = "\n".join(thetas)
                        option_count = "\n".join(option_counts)

                    else:
                        legss = newline[newline.rfind("legs"):newline.rfind('product_codes')]
                        legs = legss[legss.rfind("instrument_name")+19:legss.rfind('price')-4]
                        side = newline[newline.rfind("side")+8:newline.rfind('rep')-7]
                        #iv
                        iv = newline[newline.rfind("IV")+5:newline.rfind("IV")+11]
                        #forward price
                        forward_price = round(float(newline[newline.rfind('forward price')+16:newline.rfind('IV')-3]),2)
                        one = newline[newline.rfind("strategy"):]
                        two = one[one.rfind("rep"):]
                        if two[two.rfind("error"):] == '\n':
                            b_price = round((float(two[two.rfind("price'")+8:two.rfind("priceUSD")-3])),5)
                            #D_Mark_Price
                            d_price = round(float(two[two.rfind("priceUSD")+11:two.rfind("deltaDollar")-3]),2)
                            #deltaDollar
                            delta1 = round(float(two[two.rfind("deltaDollar")+14:two.rfind("gammaDollar")-3])*float(quantity),2)
                            delta = '$'+str(format(delta1,","))
                            #gammaDollar
                            gamma1 = round(float(two[two.rfind("gammaDollar")+14:two.rfind("vegaDollar")-3])*float(quantity),2)
                            gamma = '$'+str(format(gamma1,","))
                            #vegaDollar
                            vega1 = round(float(two[two.rfind("vegaDollar")+13:two.rfind("thetaDollar")-3])*float(quantity),2)
                            vega = '$'+str(format(vega1,","))
                            #thetaDollar
                            theta1 = round(float(two[two.rfind("thetaDollar")+14:two.rfind("thetaPercRef")-3])*float(quantity),2)
                            theta = '$'+str(format(theta1,","))
                            #option_count
                            option_count = round(float(two[two.rfind("optionCount")+14:two.rfind("optionCount")+30])*float(quantity),2)
                            """ 
                            if side == 'SELL':
                                b_price = -b_price
                                d_price = -d_price
                                delta = -delta
                                gamma = -gamma
                                vega = -vega
                                theta = -theta
                                option_count = -option_count
                            else:
                                b_price = b_price
                                d_price = d_price
                                delta = delta
                                gamma = gamma
                                vega = vega
                                theta = theta
                                option_count = option_count  """
                        else:
                            b_price = None
                            d_price = None
                            delta = None
                            gamma = None
                            vega = None
                            theta = None
                            option_count = None
                            
                    #write
                    data_ev['role'] = role
                    data_ev['side'] = side
                    data_ev['quantity'] = quantity  
                    data_ev['description'] = description
                    data_ev['legs'] = legs                                           
                    data_ev['b_mark_price'] = b_price
                    data_ev['d_mark_price'] = d_price
                    data_ev['iv'] = iv
                    data_ev['forward_price'] = forward_price
                    data_ev['delta'] = delta
                    data_ev['gamma'] = gamma
                    data_ev['vega'] = vega
                    data_ev['theta'] = theta
                    data_ev['state'] = state
                    data_ev['closed_reason'] = closed_reason
                    data_ev['creat_time'] = created_at_time
                    data_ev['last_update_time'] = last_updated_at_time
                    data_ev['option_count'] = option_count
                    data_ev['product_id'] = product_id
                    data_all[count] = data_ev
                    log2.info(f"----Completed {count} lines-----")
                    count += 1
                except Exception as e:
                    log2.error(f'Error reading data:{str(e)}')
                    continue
        
    count = 1                
    for k,v in data_all.items():      
        log2.info(f"Start inserting the {count} data")
        try:
            access_ = Access(Role=v["role"],Side=v["side"],Qty=v["quantity"],Description=v["description"],Legs=v["legs"],
                            B_Mark_price=v["b_mark_price"],D_Mark_price=v["d_mark_price"],Mark_IV=v["iv"],Forward_Price=v["forward_price"],
                            Delta_Dollar=v["delta"],Gamma_Dollar=v["gamma"],Vega_Dollar=v["vega"],Theta_Dollar=v["theta"],
                            State=v["state"],Close_Reason=v["closed_reason"],
                            Create_Time=v["creat_time"],Last_Update_Time=v["last_update_time"],
                            Option_Count=v["option_count"],Product_Id=v["product_id"])
            access_t=Access.check_existing(access_)#Ensure that the same data is not repeatedly inserted
            session.add(access_t)
            session.commit()
            log2.info(f"Inserting the {count} data")
        except Exception as e:
            log2.error(f"Inserting the {count} data: {str(e)}")      
        count += 1

    log2.info("Successfully inserted the all LOG data, write db done")

if __name__=="__main__":
    adddb()

