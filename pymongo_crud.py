import pymongo
import pandas as pd
import numpy as np
import xlwings as xw
import datetime 
from pandas.tseries.offsets import BDay
import json
import opstrat as op
import matplotlib.pyplot as plt
import time
import re
#import argparse

#client = pymongo.MongoClient("mongodb+srv://<username>:<password>@<cluster>.mongodb.net/paper_trades?retryWrites=true&w=majority")
client = pymongo.MongoClient("mongodb+srv://kyz128:z12081120Ykim@cluster0.po32h.mongodb.net/paper_trades?retryWrites=true&w=majority")
db = client.paper_trades
wb = xw.Book('excel_interface.xlsm')

##############################################################################################################################################################

# Flash upload and visualization 

##############################################################################################################################################################

def snapshot_graph():
    top_lvl = pd.DataFrame(list(db.snapshots.find({}, {'date': 1, 'flash_amt': 1, '_id':0})))
    details = list(db.snapshots.find({}, {'date': 1, 'flash_details': 1, '_id':0}))
    edata = pd.json_normalize(details, record_path =['flash_details'], meta=['date'])
    cday = wb.sheets['Chart'].range('P4').value
    if cday == None:
        today = datetime.datetime.now().replace(minute=0, hour=0, second=0, microsecond=0)
        if BDay().is_on_offset(today):
            wb.sheets['Chart'].range('P4').value = today
        else: 
            today = today - BDay(1)
            wb.sheets['Chart'].range('P4').value = today
        cday = today
    
    #Daily flash table
    #cday = datetime.datetime.strptime(sdate, '%m/%d/%Y')
    nday = cday + datetime.timedelta(days=1)
    table_res = edata.loc[(edata['date']>= cday) & (edata['date']< nday)].drop(['date'], axis=1)
    wb.sheets['Chart'].range('M16:S28').clear_contents()
    wb.sheets['Chart'].range('M16').options(index = False, header = False).value = table_res
    #if there is existing ticker selected, then don't recreate the dropdown 
    #if date has changed, clear out dropdown value so it will load the appropriate tickers for that date
    if wb.sheets['Chart'].range('P6').value == None:
        open_tickers = list(table_res['ticker'])
        dropdown_val = ",".join(open_tickers)
        wb.sheets['Chart'].range('P6').api.Validation.Add(Type=3, Formula1=dropdown_val)
        wb.sheets['Chart'].range('P6').value = open_tickers[0]
    wb.sheets['Chart']['P6'].api.HorizontalAlignment = xw.constants.HAlign.xlHAlignCenter
    
    # Total flash amt everyday
    top_fig = plt.figure()
    plt.plot(top_lvl["date"], top_lvl["flash_amt"])
    plt.xlabel('Date')
    plt.ylabel('PnL Flash ($)')
    plt.title("PnL Flash Over Time")
    plt.xticks(rotation=90)
    wb.sheets['Chart'].pictures.add(top_fig, name = "pnl", update=True, anchor=wb.sheets['Chart'].range('B3'))
    
    #Gamma vs Theta chart
    dropdown_value = wb.sheets['Chart'].range('P6').value
    cdata = edata.loc[edata['ticker'] == dropdown_value, ["date", "gamma_delivery", "theta_flash"]]
    greek_fig= plt.figure()
    plt.plot(cdata["date"], cdata["gamma_delivery"], label = "Gamma")
    plt.plot(cdata["date"], cdata["theta_flash"], label = "Theta")
    plt.xlabel("Date")
    plt.ylabel("Flash ($)")
    plt.title("{} Gamma vs Theta".format(dropdown_value))
    plt.xticks(rotation=90)
    plt.legend()
    wb.sheets['Chart'].pictures.add(greek_fig, name = "greeks", update = True, anchor=wb.sheets['Chart'].range('B30'))
    
    #Flash breakdown chart
    break_fig = plt.figure()
    plt.bar(table_res['ticker'], table_res['total_flash'])
    plt.xlabel("Ticker")
    plt.ylabel("FLash ($)")
    plt.title("{} Flash Breakdown by Ticker".format(cday.strftime("%m/%d")))
    wb.sheets['Chart'].pictures.add(break_fig, name = "breakdown", update = True, anchor=wb.sheets['Chart'].range('M30'))


def insert_snapshot():
    last_row = wb.sheets['Flash'].range('A' + str(wb.sheets['Flash'].cells.last_cell.row)).end('up').row
    df= wb.sheets['Flash'].range('A1:G%s' % last_row).options(pd.DataFrame).value.reset_index()
    jdata = json.dumps([row[["ticker", "total_flash", "delta_live", "delta_flash", "gamma_delivery", "vega_flash", "theta_flash"]].dropna().to_dict() for index,row in df.iterrows()])
    data = json.loads(jdata)
    data = {'date': datetime.datetime.utcnow(), 'flash_amt': df['total_flash'].sum(), 'flash_details': data}
    db.snapshots.insert_one(data)

##############################################################################################################################################################

# DML operations

##############################################################################################################################################################

def insert_transactions():
    #must include header 
    df = wb.app.selection.options(pd.DataFrame, index = 0).value
    df['expiry'] = df['expiry'].fillna('')
    df['expiry'] = df['expiry'].astype(str)
    jdata = json.dumps([row.dropna().to_dict() for index,row in df.iterrows()])
    data = json.loads(jdata)
    for i in data:
        i['start_date'] = datetime.datetime.utcnow()
        if i['expiry'] != "":
            try:
                i['expiry'] = datetime.datetime.strptime(i['expiry'], '%Y-%m-%d')
            except:
                pass
        else:
            i.pop('expiry', None)
    c = db.transactions.insert_many(data)
    wb.app.selection.clear_contents()
    wb.sheets['Insert_Update'].range('A1').value = ["ticker", "position", "start_price", "c/p", "strike", "expiry"]
    fetch_open()

def update_transaction(start_date, ticker, new_values, cp, strike, expiry, unset = False):
    try:
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    except:
        start = start_date
    end = start + datetime.timedelta(days=1)
    if cp == None:
        query = {"ticker": ticker, "start_date": {'$lte': end, '$gte': start - datetime.timedelta(minutes= 1)}}
    else:
        try:
            estart = datetime.datetime.strptime(expiry, '%Y-%m-%d')
        except:
            estart = expiry
        eend = estart + datetime.timedelta(days=1)
        query = {"ticker": ticker, "start_date": {'$lte': end, '$gte': start - datetime.timedelta(minutes= 1)}, "c/p": cp, "strike": strike, "expiry": {'$lte': eend, '$gte': estart}}
    if unset == False:
        c = db.transactions.update_one(query, [{"$set": new_values}])
    else:
        c = db.transactions.update_one(query, {"$unset": new_values})
    assert(c.matched_count == 1)

def close_transaction():
    #start_date of form 1-2 digit month/2 digit day/4 digit year
    transaction = wb.app.selection.value
    start_date, ticker, position, start_price, cp , strike, expiry, close_price, close_position = transaction[0], transaction[1], transaction[2], transaction[3], transaction[4], transaction[5], transaction[6], transaction[7], transaction[8]
    close_date = datetime.datetime.utcnow()
    # For options, if exercise close price = price of underlying
    # else close_price = price of option
    if expiry != None and close_date >= expiry:
        if cp == "c":
            pnl = np.round(max(close_price - strike, 0) - start_price, 2)*close_position
        else:
            pnl = np.round(max(strike - close_price, 0) - start_price, 2)*close_position
    else:
        pnl = np.round((close_price - start_price),2)*close_position

    if abs(close_position) == abs(position):
        try:
            start = datetime.datetime.strptime(start_date, '%m/%d/%Y')
        except:
            start = start_date
        end = start + datetime.timedelta(days=1)
        #c = db.transactions.delete_one({"start_date": {'$lte': end, '$gte': start}, "ticker": ticker, "position": int(position)})
        c = db.transactions.delete_one({"ticker": ticker, "position": int(position)})
        assert(c.deleted_count == 1)
        if cp == None:
            db.past_trans.insert_one({"start_date": start_date, "ticker": ticker, "position": position, "start_price": start_price,"close_date": close_date, 
                "close_price": close_price, "pnl": pnl})
        else:
            db.past_trans.insert_one({"start_date": start_date, "ticker": ticker, "position": position, "start_price": start_price, "c/p": cp, "strike": strike, "expiry": expiry, "close_date": close_date, 
                "close_price": close_price, "pnl": pnl})
    else:
        #insert into past_trans
        #update old one
        db.past_trans.insert_one({"start_date": start_date, "ticker": ticker, "position": close_position, "start_price": start_price,"close_date": close_date, "close_price": close_price, "pnl": pnl})
        # new = {"close_date": close_date, "close_price": close_price, "pnl": {"$round": [{"$multiply": [close_position, {"$subtract": [close_price, "$start_price"]}]}, 2]}, "position": {"$subtract": ["$position", close_position]}}
        new = {"position": {"$subtract": ["$position", close_position]}}
        update_transaction(start_date, ticker, new, cp, strike, expiry)
    fetch_open()

def correct_transaction():
    transaction = wb.app.selection.options(np.array, ndim=2).value
    t = np.where(pd.notnull(transaction[1]), transaction[1], None)
    ticker, start_date, cp, strike, expiry = t[0], t[1], t[2], t[3], t[4]
    unset_idx = np.argwhere(pd.isnull(transaction[1][5:])).flatten()
    if len(unset_idx) != 0:
        unset_idx = unset_idx + 5
        unset_new = dict(zip(transaction[0][unset_idx], transaction[1][unset_idx]))
        update_transaction(start_date, ticker, unset_new, cp, strike, expiry, True)
    else:
        set_idx = np.setdiff1d(np.arange(len(transaction[0])), np.append(unset_idx, [0,1,2,3,4]))
        set_new = dict(zip(transaction[0][set_idx], transaction[1][set_idx]))
        #print(start_date)
        update_transaction(start_date, ticker, set_new, cp, strike, expiry)
    wb.app.selection.clear_contents()
    wb.sheets['Insert_Update'].range('H1').value = ["ticker", "start_date", "c/p", "strike", "expiry"]
    fetch_open()

def delete_transactions(timeframe=1):
    transaction = wb.app.selection.options(np.array, ndim=2).value
    query = dict(zip(transaction[0], transaction[1]))
    if 'start_date' in transaction[0]:
        idx = np.where(transaction[0] == 'start_date')[0][0]
        try:
            start = datetime.combine(transaction[1][idx], datetime.min.time())
        except:
            start = transaction[1][idx]
        if timeframe != "inf":
            end = start + datetime.timedelta(days=timeframe)
            query['start_date'] = {'$lte': end, '$gte': start}
        else:
            query['start_date'] = {'$gte': start}
    #print(query)
    c = db.transactions.delete_many(query)
    print(c.deleted_count)
    fetch_open()
    wb.sheets['Delete'].clear_contents()
    

##############################################################################################################################################################

# Flash and Risks

##############################################################################################################################################################

def calc_imp_vol(row):
    prev_day = (datetime.datetime.today() - BDay(1)).strftime("%Y%m%d")
    if row["c/p"] == 'c' or row["c/p"] == 'p':
        if row["type"] == "equity":
            #override_field: opt_valuation_dt, format YYYYMMDD
            return """=BDP("{} {} {}{} equity",  "ivol_tm", "opt_valuation_dt", "{}")""".format(row['ticker'], row['expiry'].strftime("%m/%d/%y"), row["c/p"], row['strike'], prev_day)
        else:
            #override field: reference_date
            return """=BDP("{} {}",  "sp_vol_surf_mid", "reference_date", "{}")""".format(row["ticker"], row["type"], prev_day)
    else:
        return np.nan

def calc_vol_chg(row):
    prev_day = (datetime.datetime.today() - BDay(1)).strftime("%Y%m%d")
    if row['c/p'] == 'c' or row['c/p'] == 'p':
        if row['type'] == 'equity':
            return """=BDP("{} {} {}{} equity",  "opt_imp_vol_pct_chng", "opt_valuation_dt", "{}")""".format(row['ticker'], row['expiry'].strftime("%m/%d/%y"), row['c/p'], row['strike'], prev_day)
        else:
            return """=BDP("{}v3m {}",  "chg_pct_1d")""".format(row["ticker"], row["type"])
    else:
        return np.nan

def greeks(row):
    #make sure date is changed
    prev_day = (datetime.datetime.today() - BDay(1)).strftime("%Y%m%d")
    if row['c/p'] == 'c' or row['c/p'] == 'p':
        if row['type'] == 'equity':
            delta = """=BDP("{} {} {}{} equity",  "delta", "opt_valuation_dt", "{}")""".format(row['ticker'], row['expiry'].strftime("%m/%d/%y"), row['c/p'], row['strike'], prev_day)
            gamma = """=BDP("{} {} {}{} equity",  "gamma", "opt_valuation_dt", "{}")""".format(row['ticker'], row['expiry'].strftime("%m/%d/%y"), row['c/p'], row['strike'], prev_day)
            vega = """=BDP("{} {} {}{} equity",  "vega", "opt_valuation_dt", "{}")""".format(row['ticker'], row['expiry'].strftime("%m/%d/%y"), row['c/p'], row['strike'], prev_day)
            theta = """=BDP("{} {} {}{} equity",  "opt_theta", "opt_valuation_dt", "{}")""".format(row['ticker'], row['expiry'].strftime("%m/%d/%y"), row['c/p'], row['strike'], prev_day)
            return [delta, gamma, vega, theta]
        else:
            #make_sure rf rate is yesterday's
            rf = wb.sheets['Summary'].range('B1').value
            #time remain also 1 day off
            t = (row['expiry'] - datetime.datetime.now()).days + 1
            st = float(row['current_price']) - float(row['price_change'])
            #assume notional = 100
            bsm = op.black_scholes(K=row['strike'], St=st, r=rf, t=t, v=row['implied_vol'], type=row['c/p'])
            greeks = bsm['greeks']
            # vega*100 so it is in terms of percent
            return [greeks['delta'], greeks['gamma']/100, greeks['vega'], greeks['theta']]          
    else:
        return [1,0,0,0]

def calc_flash():
    last_row = wb.sheets['Summary'].range('A' + str(wb.sheets['Summary'].cells.last_cell.row)).end('up').row
    df= wb.sheets['Summary'].range('A3:N%s' % last_row).options(pd.DataFrame).value.reset_index()
    df['gamma_delivery'] = np.where(df['gamma']==0, 0, df['gamma']*df['price_change']*df['position'])
    df['delta_live'] = df['delta']*df['position'] + df['gamma_delivery']
    df['delta_flash'] = df['delta_live']*df['price_change']
    df['vega_flash'] = np.where(df['vega']==0, 0, df['vega']* df['vol_pct_chg']*df['position'])
    df['theta_flash'] = np.where(df['theta']==0, 0, df['theta']*df['position'])
    df['total_flash'] = np.where(df['theta']==0, df['delta_flash'], df['delta_flash'] + df['vega_flash'] + df['theta_flash'])
    final = df.groupby(["ticker"]).agg({'delta_live': 'sum', 'gamma_delivery': 'sum', 'delta_flash': 'sum', 'vega_flash': 'sum', 'theta_flash': 'sum', 'total_flash': 'sum'})
    final.reset_index(inplace=True)
    wb.sheets['Flash'].range('A2:G%s' % last_row).clear_contents()
    wb.sheets['Flash'].range('A2').options(index = False, header=False).value = final

def fetch_open():
    data = list(db.transactions.find())
    df = pd.DataFrame(data)
    df = df.reindex(columns= ['_id', 'start_date', 'ticker', 'position', 'start_price', 'c/p', 'strike', 'expiry',
                              'close_price','close_position']).drop(['_id'], axis = 1)
    #print(df)
    last_row = wb.sheets['Open_Transactions'].range('A' + str(wb.sheets['Open_Transactions'].cells.last_cell.row)).end('up').row
    wb.sheets['Open_Transactions'].range('A1:I%s' % last_row).clear_contents()
    wb.sheets['Open_Transactions'].range('A1').options(index=False).value = df

def fetch_past():
    data = list(db.past_trans.find())
    df = pd.DataFrame(data)
    df = df.reindex(columns= ['_id', 'start_date', 'ticker', 'position', 'start_price', 'c/p', 'strike', 'expiry',
                              'close_date', 'close_price','pnl']).drop(['_id'], axis = 1)
    #print(df)
    last_row = wb.sheets['Past_Transactions'].range('A' + str(wb.sheets['Past_Transactions'].cells.last_cell.row)).end('up').row
    wb.sheets['Past_Transactions'].range('A1:J%s' % last_row).clear_contents()
    wb.sheets['Past_Transactions'].range('A1').options(index=False).value = df


def retrieve_risk():
    data = list(db.transactions.find())
    df = pd.DataFrame(data)
    df = df.groupby(["ticker", "c/p", "strike", "expiry"], dropna= False).agg({'position': 'sum'})
    df.reset_index(inplace=True)
    df['type'] = np.where(df['ticker'].str.len() == 6, 'curncy', 'equity')
    df["current_price"] = df[["ticker", "type"]].apply(lambda x: """=BDP("{} {}",  "px_last")""".format(x["ticker"], x["type"]), axis = 1) 
    df["price_change"] = df[["ticker", "type"]].apply(lambda x: """=BDP("{} {}",  "chg_net_1d")""".format(x["ticker"], x["type"]), axis = 1)
    #print(df['expiry'])
    df["implied_vol"]= df.apply(calc_imp_vol, axis=1)
    df["vol_pct_chg"]= df.apply(calc_vol_chg, axis=1)
    wb.sheets['Summary'].range('A4').options(index = False, header = False).value = df
    last_row = wb.sheets['Summary'].range('A' + str(wb.sheets['Summary'].cells.last_cell.row)).end('up').row
    val_df= wb.sheets['Summary'].range('A3:J%s' % last_row).options(pd.DataFrame).value.reset_index()
    time.sleep(30)
    val_df[["delta", "gamma", "vega", "theta"]] = val_df.apply(greeks, axis=1, result_type="expand")
    #val_df[["current_price", "price_change", "implied_vol", "vol_pct_chg"]] = df[["current_price", "price_change", "implied_vol", "vol_pct_chg"]]
    wb.sheets['Summary'].range('A4:N%s' % last_row).clear_contents()
    wb.sheets['Summary'].range('A4').options(index = False, header = False).value = val_df
    return val_df

	
# if __name__ == "__main__":
# 	parser = argparse.ArgumentParser(description='Run MongoDB CRUD operations.')
# 	subparsers = parser.add_subparsers()

# 	parser_summary = subparsers.add_parser('summary', help = "Display net shares and net positions.")
# 	parser_summary.set_defaults(func = calculate_all_net_position)
# 	"""
# 	example call:
# 	>python pymongo_crud.py summary "{\"EURUSD\": 1.23, \"XAGUSD\": 28.30}"
#     {'XAGUSD': [100, 269.0], 'EURUSD': [50, 0.5]}
# 	"""
# 	parser_insert = subparsers.add_parser('insert', help = "Insert a transaction into Mongo Atlas.")
# 	parser_insert.set_defaults(func = insert_transactions)

# 	parser_close = subparsers.add_parser('close', help = "Close an open transaction.")
# 	parser_close.set_defaults(func = close_transaction)

# 	parser_correct = subparsers.add_parser('correct', help = "Correct a transaction.")
# 	parser_correct.set_defaults(func = correct_transaction)

# 	parser_show = subparsers.add_parser('show_all', help = "Display all transactions.")
# 	parser_show.set_defaults(func = fetch_all)

# 	parser_open_ticker = subparsers.add_parser('open_tickers', help = "Display the unique tickers of all open transactions.")
# 	parser_open_ticker.set_defaults(func = get_open_tickers)

# 	args = parser.parse_args()
# 	command_args = vars(args).copy()
# 	del command_args['func']
# 	args.func(**command_args)
