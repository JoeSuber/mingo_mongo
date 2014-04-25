__author__ = 'suber1'
"""
1) allow import of all csv-style data via user created templates
2) templates are stored in database along with the data and a record of imported file-names
3) templates can be found by a header-string that matches against the incoming header
    if no match, user is prompted to provide the mapping of headers-to-db-collection-columns

"""
from pymongo import MongoClient
import os
from pprint import pprint
import cPickle


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
        primaries = {num: unicode(pri) for num, pri in enumerate(primarydb.collection_names())}
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


class CsvMapped(dict):
    """
    Presents persistent pairing (& pickling) of parsed CSV-parts to Mongodb
    collection dicts.  Mappings can be defined here and also load, if available,
    pickled mappings created earlier during interactive matching process. Goal is
    to map a csv-source file to database once & from that time forward, have
    similar documents automatically recognized, even if old database is unavailable.
    """
    def __init__(self, atlas={}, mapfile='passed_in_csv_headers.pkl'):
        """
        :type atlas: dict
        """
        try:
            assert(isinstance(atlas, dict))
        except AssertionError:
            print("If used, the atlas param should be a dict mapping csv-file-types")
            print("to dictionaries like: {csv-header: db-category, ...}")
            print("A non-dict was passed to CsvMapped via atlas: ")
            pprint(atlas)
            exit(0)
        if len(atlas):
            for elem in atlas.viewvalues():
                try:
                    assert(isinstance(elem, dict))
                except AssertionError:
                    print("A value of an atlas-value is not a dict: ")
                    pprint(elem)
                    exit(0)
            # all good? preserve passed in atlas for later backtracking
            with open(mapfile, 'wB') as mapfob:
                cPickle.dump(atlas, mapfob, cPickle.HIGHEST_PROTOCOL)
        else:
            # if atlas is empty, open pickle if we can
            if os.path.exists(mapfile):
                try:
                    with open(mapfile, 'rB') as mapfob:
                        atlas = cPickle.load(mapfob)
                except IOError:
                    print(" {} was given as a pickle-file for atlas-csv-header-maps, ".format(mapfile))
                    print(" but it isn't a pickle.  Moving on...")
        self.atlas = atlas
        self.pickle_fn = mapfile
        self.thetext = ''

    def fn_ctime(self, fn):
        """
        attach c_time os-provided info to filename in a consistent way
        """
        return unicode(fn + '|' + unicode(os.stat(fn).st_ctime))


    def csvsources(self, usedcsvdb, startdir=None, looking_for='*.csv'):
        """
        gather and track the input data (saved in csv format) for
         later importation into the mongodb. Return only valid choices as dict.
        """
        if not startdir:
            startdir = os.path.expanduser('~')

        # paths to CSV files,
        csvfiles = [os.path.join(root, filename)
                    for root, dirnames, filenames in os.walk(startdir)
                    for filename in filenames if filename.endswith(looking_for)]
        fn_dd = {ctr: fn for ctr, fn in enumerate(csvfiles)}

        # create unique key for filename to prevent re-import of the same data
        longfnkey = {}
        for num, fnv in fn_dd.viewitems():
            longfnkey.update({self.fn_ctime(fnv): num})

        # check already used 'imported_flnms' database against glob filenames
        alreadyused = []
        examining = longfnkey.keys()
        for checking in usedcsvdb.find():
            # todo: don't forget to update database with filenames we use later!
            if checking in examining:
                deadnum = longfnkey[checking]
                alreadyused.append(deadnum)
                print('{:4} Already Imported: {}'.format(deadnum, fn_dd.pop(deadnum)))
        print("There are {} files with extension {} we have already imported into database"
              .format(len(alreadyused), looking_for))
        # re-index choices into consecutive digits
        # add in an option to not do anything
        showfn_dd = {num: fnp for num, fnp in zip(xrange(len(fn_dd)), fn_dd.values())}
        showfn_dd.update({len(fn_dd): ' - NONE - '})
        return showfn_dd

    def construct_header(self, fpath, online=0, spliton=','):
        """
        return strung-together representation of the column headers in a csv file
        """
        with open(fpath, 'rU') as fob:
            self.thetext = fob.read().splitlines()
        return "!|!".join([h.strip() for h in self.thetext[online].split(spliton)])

    def headers_to_mongo(self, db, hstrip):
        """
        Here are the database inventory categories. Also interactively generates the
        map to be saved and referenced by the header-string.
        """
        if hstrip in self.atlas.viewkeys():
            print("Already assigned: {} ".format(hstrip))
            return self.atlas[hstrip]
        catlist = [u'date_added', u'desc_long', u'price_we_sell', u'product_code',
                   u'manufacturer', u'sale_history', u'buy_history', u'barcode', u'we_buy_price',
                   u'desc_short', u'date_modified', u'_id', u'quant_in_stock', u'quant_min',
                   u'quant_max', u'quant_on_order', u'sku', u'order_history', u'receive_hist',
                   u'increment_quant', u'decrement_quant']
        hdrlist = {kk: vv for kk, vv in enumerate(hstrip.split('!|!'))}
        hdrlist.update({len(hdrlist): ' - NO MATCH - ', len(hdrlist)+1: ' - START OVER - '})
        pprint(catlist)
        genmap = {}
        for dbcat in catlist:
            print()
            selnum, selcategory = selections(hdrlist, prompt='proper match for {}'.format(dbcat))
            if selcategory != ' - NO MATCH - ':
                if selcategory == ' - START OVER - ':
                    return self.headers_to_mongo(db, hstrip)
                genmap[selcategory] = dbcat
                hdrlist.pop(selcategory)
                if len(hdrlist) < 2:
                    break
        self.atlas[hstrip] = genmap
        with open(self.pickle_fn, 'wB') as hfob:
            cPickle.dump(self.atlas, hfob, cPickle.HIGHEST_PROTOCOL)
        return genmap

    def parsedata(self, headers, top_skip=0):
        """
        the previously read file, a list of lines, is split out by comma, assigned to dict
        """
        header_quant = len(headers)
        numer = 0
        csvdocs = []
        for numer, csvline in enumerate(self.thetext):
            if numer >= top_skip:       # skipping line zero as it should only be the headers
                lineparts = csvline.split(',')
                try:
                    assert(header_quant == len(lineparts))
                except AssertionError:
                    print('Whoa! at line {} in {} '.format(numer, fpath))
                    print('(looks like) {}'.format(csvline))
                    print(' there are {} header-keys but {} values'.format(header_quant, len(lineparts)))
                    print(' exiting now so you may fix the CSV file.')
                    exit(0)
                csvdocs.append({h: cell.strip() for h, cell in zip(headers.values(), lineparts)})
        print('{} items are to be gleaned from this CSV file: {}'.format(numer, fpath))
        return csvdocs



if __name__ == "__main__":
    #dbserverip = '192.168.0.105'
    dbserverip = 'localhost'
    dbserverport = 27017
    xmarks = CsvMapped()

    # user chooses the database and collection we are messing with:
    overalldb, stuffdb = explore(0, MongoClient(dbserverip, dbserverport))

    # previously found mappings between CSV and DB columns:
    hdrs = overalldb['headers_map']
    importedfn = overalldb['imported_flnms']

    # find database collection labels for 'explored' stuffdb
    labelset = set()
    for hp in stuffdb.find():
        labelset = set(kk[0] for kk in hp.viewitems())
    print('labelset (from things already in db): {}'.format(labelset))

    # do mappings map to database?

    # validate found source files for potential input into db
    new_fn_dd = xmarks.csvsources(importedfn, startdir=None, looking_for='*.csv')

    # now have user choose one until none left or done:
    selnum = 0
    while selnum != (len(new_fn_dd) - 1):
        selnum, fpath = selections(new_fn_dd, prompt='Above are saved CSV files you can add to mongod. Choose wisely: ')
        if selnum != (len(new_fn_dd) - 1):
            # open file, determine header
            #  side effect: text body inside CsvMapped instance
            hdrstring, np = '', 0
            while not hdrstring:
                hdrstring = xmarks.construct_header(fpath, online=np)
                np += 1
                if np > 60:
                    print("Breaking out due to a lot of blank lines instead of headers")
                    break
            print("Headline: #{:3} {}".format(np, hdrstring))
                # look up header in database to find import map
            importmap = hdrs[hdrstring].find_one()
            # if no import-map, create the import-map:
            if not importmap:
                importmap = xmarks.headers_to_mongo(hdrs, hdrstring)
                if importmap:
                    hdrs[hdrstring].insert(importmap)
                #including some rules about adding, subtracting quantities
            # using the import-map, import the data
            csvdocs = xmarks.parsedata(importmap, top_skip=np)
            # on success, add the input filename to database of imported_fn, re-run csv-sources
            if csvdocs:
                importedfn.update(hdrstring)
                stuffdb.update(csvdocs)
    """
    # assign correct info to correct keys for insertion into current db collection
        addlist = []
        for itemdd in csvdocs:
            csvstuff = {}
            # GWk come from manufacturer's csv-headers, dbv are in mongo database
            for GWk, dbv in xmarks.atlas['GAWmap'].viewitems():
                if GWk in itemdd:
                    csvstuff[dbv] = itemdd[GWk]
            addlist.append(csvstuff)
        # also we want the best fit in case some CSVs share column names
        # make report to get what we don't have

    # first fix formats and types to conform
    ctr = 0
    for item in addlist:
        FOUND = False
        try:
            item[u'cost'] = int(item[u'cost'].replace('$', "").replace('.', "") or 0)
            item[u'price'] = int(item[u'price'].replace('$', "").replace('.', "") or 0)
            item[u'product_code'] = ('GAW ' + item[u'product_code']).strip()
            #item[u'sku'] = int(item[u'sku'])
        except ValueError as Ve:
            print('*(* VALUE (*(*(*)*) ERRoR *)*)')
            pprint(item)
            pprint(Ve)
        for olditem in stuffdb.find():
            if item[u'sku'] in olditem.viewvalues():
                print('BAR FOUND: {:8} {}'.format(item[u'product_code'], item[u'name']))
                FOUND = True
                break
            elif item[u'product_code'] in olditem.viewvalues():
                print('PCD FOUND: {:8} {}'.format(item[u'product_code'], item[u'name']))
                FOUND = True
                break
        if not FOUND:
            with open('/home/suber1/Desktop/order.txt', 'ab') as ofob:
                ofob.write("{} - item: {:11} {:7} {} \n".format(ctr, item[u'sku'], item[u'price'], item[u'name']))
    """
    print("there are {} items to be added / updated".format(len(addlist)))
    # assign/map parsed out headers to current database categories
    # perhaps we just have a text file with the mappings that are valid.
    # if none are valid we will write to the file so they can be added

    print('Goodbye!')
    exit(0)
