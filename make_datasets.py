import pandas as pd
import numpy as np
import glob

#filename_queue = tf.train.string_input_producer('output_cleaned/FB_output.csv')

#reader = tf.TextLineReader()
#key, value = reader.read(filename_queue)

header = ['UnderlyingPrice', 'Type', 'DaysLeft', 'Strike', 'Volume', 'OpenInterest', 'IV', 'Delta', 'Gamma', 'Theta', 'Vega', 'PurchasePrice', 'Profit']
data_types = {
    'UnderlyingPrice': np.float64,
    'Type': str,
    'DaysLeft': np.int32,
    'Strike': np.float64,
    'Volume': np.int32,
    'OpenInterest': np.int32,
    'IV': np.float64,
    'Delta': np.float64,
    'Gamma': np.float64,
    'Theta': np.float64,
    'Vega': np.float64,
    'PurchasePrice': np.float64,
    'Profit': np.float64
}
for filename in glob.glob('output_cleaned/*.csv'):
    print filename

    df = pd.read_csv(filename, sep=',', usecols=header, dtype=data_types)

    df.replace('?', np.nan, inplace=True)
    df.dropna(inplace=True)

    cols_to_transform = header[1:2]
    df = pd.get_dummies(df, columns=['Type'])

    df.to_csv('datasets/normal/' + filename.split('/')[1], index=False)

stats = ['calliv', 'putiv', 'meaniv', 'callvol', 'putvol', 'calloi', 'putoi']
for stat in stats:
    header.append(stat)
data_types.update({
    'calliv': np.float64,
    'putiv': np.float64,
    'meaniv': np.float64,
    'callvol': np.int32,
    'putvol': np.int32,
    'calloi': np.int32,
    'putoi': np.int32
})
for filename in glob.glob('output_stats/*.csv'):
    print filename

    df = pd.read_csv(filename, sep=',', usecols=header, dtype=data_types)
    df.replace('?', np.nan, inplace=True)
    df.dropna(inplace=True)

    df = pd.get_dummies(df, columns=['Type'])
    
    df.to_csv('datasets/with_stats/' + filename.split('/')[1], index=False)

#defaults = [0.0, 'call', 0, 0.0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
#und_price, opt_type, days_left, strike, vol, oi, iv, delta, gamma, theta, vega, price, profit = tf.decode_csv(
#    value, record_defaults=defaults, select_cols=[1,3,6,7,8,9,10,11,12,13,14,15,17]
#)
#features = tf.stack([und_price, opt_type, days_left, strike, vol, oi, iv, delta, gamma, theta, vega, price])


