# Get Names of the CurrencyPiars.
# Run this script every hour to check whether Poloniex supporting new currency pairs or not.
import os
from poloniex import Poloniex
from twisted.internet import task
from twisted.internet import reactor
#from time import mktime
#from datetime import datetime

DATAROOT = 'pairs_record'

polo = Poloniex()

def get_currency_pairs():

    currency_pairs = dict()
    pairs = polo.returnTicker().keys()

    for pair in pairs:

        main_currency = pair.split('_')[0]

        if main_currency not in currency_pairs.keys():
            currency_pairs[main_currency] = []

        currency_pairs[main_currency].append(pair)

    return currency_pairs

def record_currency_pairs(currency_pairs):

    if not os.path.isdir(DATAROOT):

        os.mkdir(DATAROOT)

    for key, value in currency_pairs.iteritems():

        fnames = DATAROOT + '/' + key + '_X.csv'

        if os.path.isfile(fnames):
            # Check whether having new supporting currency pairs.
            fp = open(fnames, 'r')

            num_of_pairs = int(fp.readline().split(',')[1])

            fp.close()

            if(num_of_pairs == len(value)):
                continue;

            else:

                fp = open(fnames, 'w')

                fp.write('Number, %d\n' % len(value))

                for pair in value:

                    fp.write(pair + '\n')

                fp.close()
                 
            
        else:

            fp = open(fnames, 'w')

            fp.write('Number, %d\n' % len(value))

            for pair in value:

                fp.write(pair + '\n')

            fp.close()

currency_pairs           = get_currency_pairs()

record_currency_pairs(currency_pairs)


