__author__ = 'suber1'
"""
1) allow import of all csv-style sheets via user created templates
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

class CsvMapped(dict):
    """
    Presents persistent pairing (& pickling) of parsed CSV-parts to Mongodb
    collection dicts.  Mappings can be defined here and also load, if available,
    pickled mappings created earlier during interactive matching process. Goal is
    to map a csv-source file to database once & from that time forward, have
    similar documents automatically recognized, even if database is unavailable.
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

    def headers_to_mongo(self, db):
        """
        get the db and store the unique maps created by user so as to not rely on pickled
        """
        db.update(
                     { type: "book", item : "journal" },
                     { $set : { qty: 10 } },
                     { upsert : true }
                   )
        for kk, vv in self.atlas.viewitems():
            csvmapsdb.



if __name__ == "__main__":
    looking_for = '*.csv'
    #dbserverip = '192.168.0.105'
    dbserverip = 'localhost'
    dbserverport = 27017
    xmarks = CsvMapped()

    # user chooses the database and collection we are messing with:
    overalldb, stuffdb = explore(0, MongoClient(dbserverip, dbserverport))
    # previously found mappings between CSV and DB columns:
    hdrs = overalldb['headers_map']
    importedfn = overalldb['imported_flnms']

    # paths to CSV files,
    csvfiles = [os.path.join(root, filename)
                for root, dirnames, filenames in os.walk(os.path.expanduser('~'))
                for filename in filenames if filename.endswith(looking_for)]
    fn_dd = {ctr: fn for ctr, fn in enumerate(csvfiles)}

    # create unique key for filename to prevent re-import of the same data
    longfnkey = {}
    for num, fnv in fn_dd.viewitems():
        longfnkey.update({unicode(fnv + '|' + unicode(os.stat(fnv).st_ctime)): num})

    # check already used 'imported_flnms' database against glob filenames
    alreadyused = []
    examining = longfnkey.keys()
    for checking in importedfn.find():
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

    # now have user choose one:
    YOUMAYPASS = False
    while not YOUMAYPASS:
        selnum, fpath = selections(fn_dd, prompt='Above are saved CSV files you can add to mongod. Choose wisely: ')
        if selnum in alreadyused:
            YOUMAYPASS = False
            print("that one has already been imported. Try again")
        else:
            YOUMAYPASS = True
            print(" You chose wisely: {}".format(fpath))

    #
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

    # user chooses the kind of invoice being imported

    # assign correct info to correct keys for insertion into current db collection
    addlist = []
    for itemdd in csvdocs.viewvalues():
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

    print("there are {} items to be added / updated".format(len(addlist)))
    # assign/map parsed out headers to current database categories
    # perhaps we just have a text file with the mappings that are valid.
    # if none are valid we will write to the file so they can be added

    print('Goodbye!')
    exit(0)
