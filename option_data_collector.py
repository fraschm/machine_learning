import csv
import datetime
import pprint
import time
import glob
import pandas as pd
from datetime import timedelta
from collections import defaultdict, OrderedDict

class Option(object):
    def __init__(self, symbol, underlying_price, option_name, option_type,
        expiration, date, strike, last, bid, ask, volume, open_interest, iv,
        delta, gamma, theta, vega):
        self.symbol = symbol
        self.underlying_price = underlying_price
        self.option_name = option_name
        self.option_type = option_type
        self.expiration = expiration
        self.date = date
        self.strike = strike
        self.last = last
        self.bid = bid
        self.ask = ask
        self.volume = volume
        self.open_interest = open_interest
        self.iv = iv
        self.delta = delta
        self.gamma = gamma
        if theta == "NaN":
            self.theta = 0.0
        else:
            self.theta = theta
        self.vega = vega
        if self.last == 0 or ask < self.last:
            self.purchase_price = ask
        else:
            self.purchase_price = last
        self.expiration_price = self.purchase_price
        self.expiration_underlying = self.underlying_price

    @property
    def profit(self):
        if getattr(self, '_profit', None) is None:
            if self.option_type == 'call':
                exercise_profit = self.expiration_underlying - (self.strike + self.purchase_price)
                if self.expiration_price - self.purchase_price > exercise_profit:
                    self._profit = self.expiration_price - self.purchase_price
                else:
                    self._profit = exercise_profit
            else:
                exercise_profit = (self.strike - self.purchase_price) - self.expiration_underlying
                if self.expiration_price - self.purchase_price > exercise_profit:
                    self._profit = self.expiration_price - self.purchase_price
                else:
                    self._profit = exercise_profit
        return round(self._profit, 2)
stocks = ['BAC', 'CME', 'NAVI', 'F', 'TSLA', 'ATVI', 'FB', 'PSTG', 'DATA']

folders = ['2017_June', '2017_July', '2017_August', '2017_September', '2017_October', '2017_December', '2018_January', '2018_February', '2018_March', '2018_April', '2018_May']
option_data = defaultdict(lambda: OrderedDict())
total_time = 0
total_entries = 0
completed = 0
s = time.time()
for folder in folders:
    total_entries += len(sorted(glob.glob('ftp.deltaneutral.com/BlackScholes/' + folder + '/options_*.csv')))
for folder in folders:
    for filename in sorted(glob.glob('ftp.deltaneutral.com/BlackScholes/' + folder + '/options_*.csv')):
        start = time.time()
        print filename
        with open(filename, 'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                symbol = row['UnderlyingSymbol']
                if symbol in stocks and row['Exchange'] == '*':
                    expiration = datetime.datetime.strptime(row['Expiration'],
                        '%m/%d/%Y')
                    date = datetime.datetime.strptime(row[' DataDate'], '%m/%d/%Y')
                    if expiration < datetime.datetime.strptime('06/30/2018', '%m/%d/%Y'):
                        exp = option_data[symbol].get(expiration, [])
                        exp.append(Option(
                            row['UnderlyingSymbol'],
                            float(row['UnderlyingPrice']),
                            row['OptionSymbol'],
                            row['Type'],
                            expiration,
                            date,
                            float(row['Strike']),
                            float(row['Last']),
                            float(row['Bid']),
                            float(row['Ask']),
                            int(row['Volume']),
                            int(row['OpenInterest']),
                            float(row['IV']),
                            float(row['Delta']),
                            float(row['Gamma']),
                            float(row['Theta']),
                            float(row['Vega'])
                        ))
                        option_data[symbol][expiration] = exp
                    symbol_dic = option_data[symbol]
                    for option in symbol_dic.get(expiration, []):
                        option.expiration_underlying = float(row['UnderlyingPrice'])
                        if option.option_name == row['OptionSymbol']:
                            if float(row['Last']) == 0 or float(row['Bid']) > float(row['Last']):
                                option.expiration_price = float(row['Bid'])
                            else:
                                option.expiration_price = float(row['Last'])
        csvfile.close()
        completed += 1
        fin = time.time()
        total_time += fin - start 
        print "Finished in {:.2f} seconds".format(fin - start)
        print "completed {} of {} entries, {:.2f}% complete".format(completed, total_entries, (float(completed) / total_entries) * 100.00)
        print "Elapsed time: {}".format(str(timedelta(seconds=total_time)))
print "Total Time = {} minutes".format(str(timedelta(seconds=total_time)));
siz = 0
for _, symbol in option_data.items():
    for _, v in symbol.items():
        siz += len(v)
print "Total items: {}".format(siz)
start = time.time()
print "Creating CSV"
for symbol in stocks:
    with open('output/' + symbol + '_output.csv', 'wb+') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',')
        filewriter.writerow(['Symbol', 'UnderlyingPrice', 'OptionSymbol', 'Type', 'Expiration', 'DataDate', 'DaysLeft', 'Strike',
            'Volume', 'OpenInterest', 'IV', 'Delta', 'Gamma', 'Theta', 'Vega', 'PurchasePrice', 'ExpirationPrice', 'Profit'])
        for _, options in option_data[symbol].items():
            for option in options:
                filewriter.writerow([
                    option.symbol,
                    option.underlying_price,
                    option.option_name,
                    option.option_type,
                    option.expiration,
                    option.date,
                    (option.expiration - option.date).days,
                    option.strike,
                    option.volume,
                    option.open_interest,
                    option.iv,
                    option.delta,
                    option.gamma,
                    option.theta,
                    option.vega,
                    option.purchase_price,
                    option.expiration_price,
                    option.profit
                ])
        csvfile.close()

for symbol in stocks:
    df = pd.read_csv('output/' + symbol + '_output.csv', header=0)
    df = df.sort_values(by=['DataDate'])
    df.to_csv('output_cleaned/' + symbol + '_output.csv', index=False)
option_stats = {}
for folder in folders:
    for filename in sorted(glob.glob('ftp.deltaneutral.com/BlackScholes/' + folder + '/optionstats_*.csv')):
        with open(filename, 'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['symbol'] in stocks:
                    strdate = datetime.datetime.strptime(row['quotedate'], '%Y%m%d')
                    if strdate not in option_stats:
                        option_stats[strdate] = {}
                    option_stats[strdate][row['symbol']] = row
            csvfile.close()
pprint.pprint(option_stats)
for symbol in stocks:
    new_cols = []
    with open('output_cleaned/' + symbol + '_output.csv', 'rb') as csvfile:
        with open('output_stats/' + symbol + '_output.csv', 'wb+') as outfile:
            writer = csv.writer(outfile)
            reader = csv.reader(csvfile)

            data = []
            row = next(reader)
            new_cols = ['calliv', 'putiv', 'meaniv', 'callvol', 'putvol', 'calloi', 'putoi']
            for col in new_cols:
                row.append(col)
            data.append(row)
            for row in reader:
                datestr = datetime.datetime.strptime(row[5], '%Y-%m-%d %H:%M:%S') 
                for col in new_cols:
                    row.append(option_stats[datestr][symbol][col])
                data.append(row)    
            writer.writerows(data)
            outfile.close()
        csvfile.close()
        


print "CSV file created in {} seconds".format(time.time() - start)
