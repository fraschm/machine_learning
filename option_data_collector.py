import csv
import datetime
import pprint
import time
import glob
from collections import defaultdict

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
        if self.last == 0:
            self.purchase_price = ask
        else:
            self.purchase_price = last
        self.expiration_price = self.purchase_price

    @property
    def profit(self):
        if getattr(self, '_profit', None) is None:
            if self.option_type == 'call':
                self._profit = self.expiration_price - (self.strike\
                    + self.purchase_price)
            else:
                self._profit = (self.strike + self.purchase_price)\
                    - self.expiration_price
        return self._profit
stocks = ['BAC', 'CME', 'NAVI', 'F', 'TSLA', 'ATVI', 'FB', 'PSTG', 'DATA']

folders = ['2017_June', '2017_July', '2017_August', '2017_September', '2017_October', '2017_December', '2018_January', '2018_February', '2018_March',
    '2018_April', '2018_May', '2018_June']
files = ['options_20170801.csv']
option_data = defaultdict(list)
total_time = 0
for folder in folders:
    for filename in sorted(glob.glob('ftp.deltaneutral.com/BlackScholes/' + folder + '/options_*.csv')):
        start = time.time()
        print filename
        with open(filename, 'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['UnderlyingSymbol'] == 'AAPL' and row['Exchange'] == '*':
                    expiration = datetime.datetime.strptime(row['Expiration'],
                        '%m/%d/%Y')
                    date = datetime.datetime.strptime(row[' DataDate'], '%m/%d/%Y')
                    if expiration < datetime.datetime.strptime('06/30/2018', '%m/%d/%Y'):
                        option_data[expiration].append(Option(
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
                    for option in option_data.get(date, []):
                        if option.option_name == row['OptionSymbol']:
                            if float(row['Last']) == 0:
                                option.expiration_price = float(row['Last'])
                            else:
                                option.expiration_price = float(row['Bid'])
        csvfile.close()
        fin = time.time() - start
        total_time += fin 
        print "Finished in {} seconds".format(fin)
print "Total Time = {} minutes".format(total_time / 60);
siz = 0
for _, v in option_data.items():
    siz += len(v)
print "Total items: {}".format(siz)
start = time.time()
print "Creating CSV"
with open('output/option_output.csv', 'wb+') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',')
    filewriter.writerow(['Symbol', 'UnderlyingPrice', 'OptionSymbol', 'Type', 'Expiration', 'DataData', 'Strike',
        'Volume', 'OpenInterest', 'IV', 'Delta', 'Gamma', 'Theta', 'Vega', 'PurchasePrice', 'ExpirationPrice', 'Profit'])
    for _, options in option_data.items():
        for option in options:
            filewriter.writerow([
                option.symbol,
                option.underlying_price,
                option.option_name,
                option.option_type,
                option.expiration,
                option.date,
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
print "CSV file created in {} seconds".format(time.time() - start)
