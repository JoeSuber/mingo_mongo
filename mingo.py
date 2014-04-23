__author__ = 'suber1'
import pymongo
from pymongo import MongoClient
from collections import OrderedDict, Counter
import os
import glob
from pprint import pprint

def selections(dd=None, prompt='Choose from above'):
    """
    given dict of numbered options like {1: 'choose me', ...}
    present user with options and return the chosen integer key and value string
    """
    choice = 1
    if not dd:
        print('selections: empty choice dictionary, returning 0')
        return 0, {}
    for choice, dbnm in dd.viewitems():
        print('{:4}- {}'.format(choice+1, dbnm))
    q = 0
    while (q < 1) or (q > (choice+1)):
        if len(dd) > 1:
            q = int(str(raw_input(prompt)))
        else:
            print('only one choice... ')
            q = 1
    return q - 1, dd[q - 1]

def explore(done, client):
    """
    Allow exploration of key:value storage in existing mongod instance
    Also selects the database.collection to be used / toyed with.
    """
    thestuffdb, primarydb = None, None
    usedb_name, collection_name = None, None
    while not done:
        clientnames = {num: clnm for num, clnm in enumerate(client.database_names())}
        key, usedb_name = selections(clientnames, prompt='Choose the number of mongo db to be explored: ')
        print('okay - choice was {:3} - {} \n'.format(key+1, usedb_name))
        primarydb = client[usedb_name]
        primaries = {num: pri for num, pri in enumerate(primarydb.collection_names())}
        key, collection_name = selections(primaries, prompt='Choose the collection within {} to explore: '.format(usedb_name))
        print('okay - choice was {:3} - {} \n'.format(key+1, collection_name))
        thestuffdb = primarydb[collection_name]
        end_dd = {0: ' done', 1: ' continue exploration', 2: ' print out {}'.format(collection_name)}
        key, desire = selections(end_dd, prompt='sitting on {}.{} - Choose from : '.format(usedb_name, collection_name))
        print('okay - choice was {:3} - {} \n'.format(key+1, desire))
        done = not key
        if key > 0:
            done = 0
            if key == 2:
                for plc, item in enumerate(thestuffdb.find()):
                    print('#{:6}: {}'.format(plc, item))

    print(' @!@!@! Active mongo parts: {}.{} '.format(usedb_name, collection_name))
    return primarydb, thestuffdb

def importCSV(fn, headers):
    """
    given CSV filename and list of headers, return list of dict, where the
    list-element-dicts are ready to enter the mongo database with keys = CSV-headers
    """
    return None

def headcheck(hdrlist, prev_mapped):
    return None

GAWmap = {'SHORT CODE': u'product_code', 'BARCODE': u'sku', 'US Trade': u'cost', 'US MSRP': u'price',
          'long_description': u'name', 'Reorder': u'description'}

if __name__ == "__main__":
    looking_for = '*.csv'
    csv_dir = 'Documents'

    # user chooses the database and collection we are messing with:
    overalldb, stuffdb = explore(0, MongoClient('localhost', 27017))

    # previously found mappings between CSV and DB columns:
    hdrs = overalldb['headers_map']

    # paths to CSV files, have user choose one:
    user = os.path.join(os.path.expanduser('~'), csv_dir)
    fn_dd = {ctr: fn for ctr, fn in enumerate(glob.glob(user + os.sep + looking_for))}
    _, fpath = selections(fn_dd, prompt='Above are CSV files you can add to mongod. Choose wisely: ')
    print('opening: {}'.format(fpath))
    with open(fpath, 'rU') as fob:
        thetext = fob.read().splitlines()
    headers = thetext[0].split(',')
    header_quant = len(headers)
    print('There are {} column-headers in {}: '.format(header_quant, fpath))
    print(" |".join(headers))
    print(" so... ")

    # parse csv-file contents into dict of dict-keyed-by-csv-headers:
    numer = 0
    top_skip = 0
    csvdocs = {}
    for numer, csvline in enumerate(thetext):
        if numer > top_skip:       # skipping line zero as it should only be the headers
            lineparts = csvline.split(',')
            try:
                assert(header_quant == len(lineparts))
            except AssertionError:
                print('Whoa! at line {} in {} '.format(numer, fpath))
                print('(looks like) {}'.format(csvline))
                print(' there are {} header-keys but {} values'.format(header_quant, len(lineparts)))
                print(' exiting now so you may fix the CSV file.')
                exit(0)
            csvdocs[numer] = {h: cell.strip() for h, cell in zip(headers, lineparts)}
    print('{} items are to be gleaned from this CSV file: {}'.format(numer, fpath))

    # find current database categories
    labelset = set()
    for hp in stuffdb.find():
        labelset = set(kk[0] for kk in hp.viewitems())
    print('labelset (from things already in db): {}'.format(labelset))

    # check if a mapping already has been made fitting some CSV-derived columns
    # (skipping above)
    # assign correct info to correct keys for insertion into current db collection
    addlist = []
    for itemdd in csvdocs.viewvalues():
        csvstuff = {}
        # GWk come from manufacturer's csv-headers, dbv are in mongo database
        for GWk, dbv in GAWmap.viewitems():
            if GWk in itemdd:
                csvstuff[dbv] = itemdd[GWk]
        addlist.append(csvstuff)
    # also we want the best fit in case some CSVs share column names
    # make report to get what we don't have

    # first fix formats and types to conform
    for item in addlist:
        item[u'cost'] = int(item[u'cost'].replace('$', "").replace('.', "") or 0)
        item[u'price'] = int(item[u'price'].replace('$', "").replace('.', "") or 0)
        item[u'product_code'] = ('GAW ' + item[u'product_code']).strip()
        item[u'sku'] = int(item[u'sku'])

    pprint(addlist)
    print('***********')
    for item in addlist:
        result = stuffdb.find(item[u'sku'])
        if result:
            pprint(result)
            # add
        else:
            with open('/home/suber1/Desktop/order.txt', 'wU') as ofob:
                ofob.write(str(result))

    print("there are {} items to be added / updated".format(len(addlist)))
    # assign/map parsed out headers to current database categories
    # perhaps we just have a text file with the mappings that are valid.
    # if none are valid we will write to the file so they can be added

    print('Goodbye!')
    exit(0)
