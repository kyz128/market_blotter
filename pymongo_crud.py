import pymongo
import pandas as pd
import numpy as np
import xlwings as xw
import datetime 
import json
import matplotlib.pyplot as plt
#import argparse

client = pymongo.MongoClient("mongodb+srv://kyz128:z12081120Ykim@cluster0.po32h.mongodb.net/paper_trades?retryWrites=true&w=majority")
db = client.paper_trades
wb = xw.Book('excel_interface.xlsm')

def snapshot_graph():
	data = list(db.snapshots.find())
	df= pd.DataFrame(data)
	fig = plt.figure()
	plt.plot(df["date"], df["unrealized"], label = "Unrealized")
	plt.plot(df["date"], df["realized"], label = "Realized")
	plt.plot(df["date"], df["total"], label = "Total")
	plt.xlabel('Date')
	plt.ylabel('PnL ($)')
	plt.title("PnL Over Time")
	plt.legend()
	plt.xticks(rotation=90)
	wb.sheets['Chart'].pictures.add(fig, name = "pnl", update=True)

def insert_snapshot():
	date = datetime.datetime.utcnow()
	df = wb.app.selection.options(pd.DataFrame, index = 0).value
	if len(df) == 0:
		current_pos = 0
	else:
		current_pos = df["net_position"].sum()
	pipeline = [{"$match": {"final_pnl": {"$exists": True}}},
	{ "$group": { "_id" : None, "total" : { "$sum": "$final_pnl" }}}]
	realized = list(db.transactions.aggregate(pipeline))[0]['total']
	data = {'date': date, 'realized': realized, 'unrealized': current_pos, 'total': realized + current_pos}
	db.snapshots.insert_one(data)
    
def insert_transactions():
    #must include header 
    df = wb.app.selection.options(pd.DataFrame, index = 0).value
    jdata = df.to_json(orient = 'records')
    data = json.loads(jdata)
    for i in data:
        i['start_date'] = datetime.datetime.utcnow()
    db.transactions.insert_many(data)
    wb.app.selection.clear_contents()
    wb.sheets['Insert_Update'].range('A1').value = ["ticker", "shares", "start_price"]
    fetch_all()

def update_transaction(start_date, ticker, new_values, unset = False):
	try:
		start = datetime.datetime.strptime(start_date, '%m/%d/%Y')
	except:
		start = start_date
	start = datetime.datetime(start.year, start.month, start.day)
	end = start + datetime.timedelta(days=1)
	query = {"ticker": ticker, "start_date": {'$lte': end, '$gte': start}}
	if unset == False:
		db.transactions.update_one(query, [{"$set": new_values}])
	else:
		db.transactions.update_one(query, {"$unset": new_values})

def close_transaction():
    #start_date of form 1-2 digit month/2 digit day/4 digit year
    transaction = wb.app.selection.value
    start_date, ticker, end_date, end_price = transaction[0], transaction[1], transaction[4], transaction[5]
    if end_date == None:
        end_date = datetime.datetime.utcnow()
    else:
        end_date = datetime.datetime.strptime(end_date, '%m/%d/%Y')
    new = {"end_date": end_date, "end_price": end_price, "final_pnl": {"$round": [{"$multiply": ["$shares", {"$subtract": [end_price, "$start_price"]}]}, 2]}}
    update_transaction(start_date, ticker, new)
    data = list(db.transactions.find({"ticker": ticker, "end_date": end_date}))
    df = pd.DataFrame(data)[['start_date', 'ticker','shares','start_price','end_date', 'end_price', 'final_pnl']]
    wb.sheets['All_Transactions'][wb.app.selection.row -1, 0].options(index=False, header = False).value = df

def correct_transaction():
    transaction = wb.app.selection.options(np.array, ndim=2).value
    start_date, ticker = transaction[1][1], transaction[1][0]
    unset_idx = np.argwhere(pd.isnull(transaction[1])).flatten()
    set_idx = np.setdiff1d(np.arange(len(transaction[0])), np.append(unset_idx, [0,1]))
    if sum(set_idx) != 0:
        set_new = dict(zip(transaction[0][set_idx], transaction[1][set_idx]))
        update_transaction(start_date, ticker, set_new)
    if len(unset_idx) != 0:
        unset_new = dict(zip(transaction[0][unset_idx], transaction[1][unset_idx]))
        update_transaction(start_date, ticker, unset_new, True)
    wb.app.selection.clear_contents()
    wb.sheets['Insert_Update'].range('E1').value = ["ticker", "start_date"]
    fetch_all()

def calculate_position(record, curr_price):
    return (curr_price - record["price"])*record["shares"]

def calculate_all_net_position():
    curr_prices = wb.app.selection.options(dict).value
    res = {}
    pipeline = [{"$match": {"end_date": {"$exists": False}}},
            { "$sort" : { "ticker" : 1 } },
            {"$group": {"_id": "$ticker", 
                         "items": { "$push":  { "shares": "$shares", "price": "$start_price"}}}}]
    for record in db.transactions.aggregate(pipeline):
        net = sum(map(calculate_position, record["items"], np.repeat(curr_prices[record["_id"]], len(record["items"]))))
        res[record["_id"]] = {"net_shares":sum(item['shares'] for item in record['items']), "net_pnl":round(net, 2)}
    df = pd.DataFrame.from_dict(res, orient='index').sort_index()
    df = df[['net_shares', 'net_pnl']]
    wb.sheets['Summary'].range('C2').options(index=False, header=False).value = df

def fetch_all():
    data = list(db.transactions.find())
    df = pd.DataFrame(data)
    cols = list(df.columns.values)
    if 'end_date' in cols:
    	df = df[['start_date', 'ticker','shares','start_price','end_date', 'end_price', 'final_pnl']]
    else:
    	df = df[['start_date', 'ticker','shares','start_price']]
    wb.sheets['All_Transactions'].range('A1').options(index=False).value = df

def get_open_tickers():
	data = sorted(db.transactions.find({"end_date": {"$exists": False}}).distinct("ticker"))
	wb.sheets['Summary'].range('A2').value = [[i] for i in data]
	
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
