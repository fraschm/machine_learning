import csv
import date
import pprint

class Option(object):
    def __init__(self, symbol, underlying_price, option_name, option_type,
        expiration, date, strike, last, bid, ask, volume, open_interest, iv,
        delta, gamma, theta, vega):
        self.symbol = symbol
        self.underlying_price = underlying_price
        self.option_name = option_name
        self.option_type = option_type
        self.expiration = expiration
        self.date = data
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
        self.expiration_price = purchase_price

    @property
    def profit(self):
        if getattr(self, '_profit', None) is None:
            if self.option_type == 'call':
                self._profit = self.expiration_price - (self.strike
                    + self.purchase_price)
            else:
                self._profit = (self.strike + self.purchase_price)
                    - self.expiration_price
        return self._profit

files = ['options_20170801.csv']
option_data = {}
for fname in files:
    with open('options_20170801.csv', 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['UnderlyingSymbol'] == 'AAPL' and row['Exchange'] == '*':
                expiration = datetime.datetime.strptime(row['Expiration'],
                    '%m/%d/%Y')
                date = datetime.datetime.strptime(row['DataDate'], '%m/%d/%Y')
                if expiration < datetime.datetime.strptime('06/30/2018'):
                    option_data[expiration] = option_data.get(expiration,
                        []).append(Option(
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
                    if float(row['Last']) == 0:
                        option.expiration_price = float(row['Last'])
                    else:
                        option.expiration_price = float(row['Bid'])
pprint.pprint(option_data)
