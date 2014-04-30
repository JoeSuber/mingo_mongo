__author__ = 'suber1'
"""
1) allow import of all csv-style data via user created templates
2) templates are stored in database along with the data and a record of imported file-names
3) templates can be found by a header-string that matches against the incoming header
if no match, user is prompted to provide the mapping of headers-to-db-collection-columns
4) need some way to classify the action to be taken on a csv file:
a) adding new items, including barcode, product code, description...
b) adjusting inventory count for an item to the given number
c) adjusting inventory count +/- the given number
d) compare the csv against the database & report the differences
5) scrape / interact with alliance / southern hobby / games-workshop / wizards ordering system
"""

from pymongo import MongoClient
from pymongo.errors import BulkWriteError
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
        return 0, '- EMPTY -'
    for choice, dbnm in dd.viewitems():
        print('{:4}- {}'.format(choice + 1, dbnm))
    q = 0
    while (q < 1) or (q > (choice + 1)):
        if len(dd) > 1:
            q = int(unicode(raw_input(prompt)))
        else:
            print('only one choice.. ')
            q = 1
    return q - 1, dd[q - 1]


def createdbnames(dbd=None):
    """
my pie-in-the-sky plans to automate all the drudge and operate using real data
"""
    if not dbd:
        dbd = {u'manufacturer': {u'GAW': u'Games_Workshop', u'WIZ': u'Wizards_of_the_Coast', u'CHX': u'Chessex'},
               u'my_customers': [u'my_cust_id', u'email', u'requested_this', u'pre_paid_for', u'credit_file',
                                 u'face_rec', u'purchased', u'returned', u'ebay_notes', u'paid_on_invoice',
                                 u'shipped_out_date', u'ship_tracked', u'ship_rcvd', u'notes'],
               u'my_interns': [u'contact', u'email', u'schedule_past', u'schedule_future', u'good_things', u'bad_things'],
               u'item_sales_history': [u'we_sold_history', u'we_buy_history', u'receive_hist', u'cust_place_pre_order',
                                       u'cust_place_back_order', u'description', u'sku_for1', u'back_at_zero_dates'],
               u'store_inventory': [u'description', u'we_sell_price', u'multipack_sku',
                                    u'multipack_quant', u'multiprice', u'manufacturer', u'barcode', u'we_buy_cost',
                                    u'date_modified', u'quant_on_invoice', u'sku_for1', u'mfr_3letter', u'sku_alliance',
                                    u'sku_alt', u'increment_quant', u'decrement_quant', u'current_whole_quant',
                                    u'desired_quant', u'notes'],
               u'stocking': [u'description', u'we_bought_history', u'we_sold_history', u'date_added_toinv',
                             u'prefer_dist_list', u'quant_want_min', u'quant_want_max', u'quant_on_reorder',
                             u'dist_alerts', u'velocity'],
               u'import_headers': {u'gg|!hh|!ii|!': {u'gg': u'gogo', u'hh': u'hoho', u'ii': u'ioio'}, }, }
        return dbd


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
    collection dicts. Mappings can be defined here and also load, if available,
    pickled mappings created earlier during interactive matching process. Goal is
    to map a csv-source file to database once & from that time forward, have
    similar documents automatically recognized, even if old database is unavailable.
    """
    def __init__(self, atlas={}, mapfile='pregen_csv_headers.pkl'):
        """
:       type atlas: dict
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
                    print(" but it isn't a pickle. Moving on...")
        self.atlas = atlas
        self.pickle_fn = mapfile
        self.fpath = ''
        self.thetext = ''
        self.spltr = '|!'
        self.defmark = u'(@)'
        self.comma = ','
        self.startlooking = 'Desktop'
        self.catlist = createdbnames()[u'store_inventory']

    def fn_ctime(self, fn):
        """
        attach c_time os-provided info to filename in a consistent way
        """
        return unicode(fn + self.spltr + unicode(os.stat(fn).st_ctime))

    def csvsources(self, usedcsvdb, startdir=None, looking_for='.csv'):
        """
        gather and track the input data (received via e.g. csv format) for
        later import into the mongodb. Return only valid choices as dict.
        """
        if looking_for:
            self.looking_for = looking_for
        if not startdir:
            startdir = os.path.join(os.path.expanduser('~'), self.startlooking)
        print("started looking for {} from {}".format(looking_for, startdir))

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
        print('longfnkey.keys() =')
        pprint(examining)
        for checking in usedcsvdb.find():
            # must find appropriate place to add the chosen filename to database
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

    def construct_header(self, fpath, online=0):
        """
        return strung-together representation of the column headers in a csv file
        side effect: load into instance the whole of the text-file as a list of lines
        """
        with open(fpath, 'rU') as fob:
            self.thetext = fob.read().splitlines()
        self.fpath = fpath
        return self.spltr.join([h.strip().replace('.', '')
                                for h in self.thetext[online].split(self.comma)])

    def headers_to_mongo(self, db, hstrip):
        """
        Interactively generates the import-map to be saved and referenced by the header-string.
        """
        if hstrip in self.atlas.viewkeys():
            print("Already assigned: {} ".format(hstrip))
            return self.atlas[hstrip]
        hdrlist = {kk: vv for kk, vv in enumerate(hstrip.split(self.spltr))}
        catchoice = {kk: vv for kk, vv in enumerate(self.catlist)}
        catstart = len(catchoice)
        catchoice.update({len(catchoice): ' - NOT USED - ', len(catchoice)+1: ' - START OVER - ',
                          len(catchoice)+2: ' - FILL with something'})
        pprint(catchoice)
        catcutoff = len(catchoice) - catstart
        genmap = {}
        total = len(hdrlist)
        for togo, (dbhdrkey, dbhdr) in enumerate(hdrlist.viewitems()):
            print('.........................................................')
            print('of {} columns in csv-file, we still must assign {} a place'.format(total, total - togo))
            selnum, selcategory = selections(catchoice,
                                             prompt='- HEADER MAP - Select Match for |{}| : '.format(dbhdr))
            if selcategory != ' - NOT USED - ':
                if selcategory == ' - START OVER - ':
                    return self.headers_to_mongo(db, hstrip)
                genmap[selcategory] = dbhdr
                catchoice.pop(selnum)
                # auto-done if only things left are '- NO MATCH -', '- START OVER -', etc
                if len(catchoice) < catcutoff:
                    print("We ran out of database categories before exhausting {} columns".format(self.looking_for))
                    print("Now filling in defaults for remaining {} csv-columns...".format(total - togo))
                    for dbhdr in hdrlist.viewitems():
                        if dbhdr not in genmap:
                            genmap[dbhdr] = dbhdr
                    break
            else:
                # selcategory == ' - NOT USED - ', but we import anyway under csv-given column-name
                genmap[dbhdr] = dbhdr
                print("importing {} column '|{}|' as: |{}|".format(selcategory, dbhdr, dbhdr))

        # finish out map with user-defined uniform default values
        if len(catchoice):
            for leftnum, leftover in catchoice.viewitems():
                if leftnum < (len(catchoice) - catcutoff):
                    genmap[leftover] = self.defmark + unicode(raw_input(
                        " ALL '{}' will have a value = : ".format(leftover))).decode()

        # assign just-generated header-map to the key=strung-together version of the csv-top-line
        self.atlas[hstrip] = genmap
        with open(self.pickle_fn, 'wB') as hfob:
            cPickle.dump(self.atlas, hfob, cPickle.HIGHEST_PROTOCOL)
        return genmap

    def ask_where_join(self, lpl):
        """
        The unfortunate result of stray line-splitters surrounded by extra
        quote delimiters in header-text-fields must be repaired
        """
        qlpl = {num: pp for num, pp in enumerate(lpl)}
        pnum1, part1 = selections(qlpl, prompt='choose first part to join : ')
        pnum2, part2 = selections(qlpl, prompt='okay! now choose second part : ')
        qlpl[pnum1] = part1 + part2
        qlpl.pop(pnum2)
        opl = []
        for n in xrange(len(qlpl)+1):
            if n in qlpl:
                opl.append(qlpl[n])
        return opl

    def decomma_quotes(self, line):
        """
        remove splitter from between quotes ONLY
        """
        if '"' in line:
            h, q, t = line.partition('"')
            if '"' in t:
                h2, q, t2 = t.partition('"')
                h2 = h2.replace(self.comma, " ")
                return "".join([h, q, h2, q, t2])
        return line

    def parsedata(self, headers, top_skip=0):
        """
        the previously read file, a list of lines, is
        commonly split out by comma or other delimiter, assigned to dict
        """
        extra_defs = []
        try:
            headers.pop(u'_id')
        except Exception as e:
            print("passed in headers probably lacked '_id' ")
            print("but they were cute so I let them in anyway... Error = {}".format(e))
        for val in headers.viewvalues():
            if isinstance(val, unicode) and (self.defmark in val):
                val.replace(self.defmark, "")
                if val == "":
                    pass
                else:
                    extra_defs.append(val)
                    print "parsedata says extra val is: ", val
        header_quant = len(headers)
        numer = 0
        csvdocs = []
        for numer, csvline in enumerate(self.thetext):
            # often skipping line zero as it should be the header-line
            if numer >= top_skip:
                csvline = self.decomma_quotes(csvline)
                lineparts = csvline.split(self.comma)
                #print ("lineparts: {}".format(lineparts))
                #print ("extra_defs: {}".format(extra_defs))
                lineparts.extend(extra_defs)
                #print "lineparts after extend: ", lineparts
                try:
                    assert(header_quant == len(lineparts))
                except AssertionError:
                    print('Whoa! at line {} in {} '.format(numer, self.fpath))
                    print('(looks like) {}'.format(csvline))
                    print(' there are {} header-keys but {} values'.format(header_quant, len(lineparts)))
                    if header_quant < len(lineparts):
                        lineparts = self.ask_where_join(lineparts)
                    else:
                        print(' With more column-names than columns there must be something wrong')
                        print(' with either the import-map or the imported file.')
                        print(' Exiting now so you may fix the CSV file.')
                        exit(0)
                except TypeError as te:
                    print("ln: {} - {}".format(numer, csvline))
                    print("{} ".format(te))
                    print("skipped over the above line")
                    continue
                csvdocs.append({h: cell.strip() for h, cell in zip(headers.values(), lineparts)})

        print(' {} items are to be gleaned from this CSV file: {} '.format(numer, self.fpath))
        return csvdocs


if __name__ == "__main__":
    #dbserverip = '192.168.0.105'
    dbserverip = 'localhost'
    dbserverport = 27017
    dbmap = createdbnames()
    xmarks = CsvMapped(atlas=dbmap[u'import_headers'])

    # check on existence of, create if required, and make backup copies of
    # some 'databases' and 'collections' in the MongoClient
    client = MongoClient(dbserverip, dbserverport)
    currentdb = client.database_names()
    # dbmap is the hard-coded idea of the structure returned by createdbnames()
    col = ()
    for dbnm, dbvals in dbmap.viewitems():
        dbb = client[dbnm]
        print "dbb= ", dbb
        if isinstance(dbvals, list):
            col = dbb[dbnm].insert({asis: "" for asis in dbvals})
            print "col from list= ", col, dbvals
        elif isinstance(dbvals, dict):
            col = dbb[dbnm].insert(dbvals)
            print "col from dict= ", col, dbvals
        print("added database: {} w/ collection:".format(dbb))
        pprint(dbvals)
    print("created / verified databases named: ")
    print(client.database_names())

    # user chooses the database and collection we are messing with:
    overalldb, stuffdb = explore(0, client)

    # previously found mappings between CSV and DB columns:
    hdrs = overalldb[u'import_headers']
    importedfn = overalldb[u'imported_flnms']

    # find database collection labels for 'explored' stuffdb
    labelset = set()
    for hp in stuffdb.find():
        labelset = set(kk[0] for kk in hp.viewitems())
    print('labelset (from things already in db): ')
    pprint(labelset)

    # validate found source files for potential input into db
    new_fn_dd = xmarks.csvsources(importedfn, startdir=None, looking_for='.csv')

    # now have user choose one until none left or done:
    selnum = 0
    while not selnum:
        selnum, fpath = selections(new_fn_dd, prompt='Above are saved CSV files you can add to mongod. Choose wisely: ')
        if fpath == " - NONE - ":
            print("All done!")
            break
        if selnum != (len(new_fn_dd) - 1):  # ie, the last choice, - DONE -
            # open file, determine header
            # side effect: text body inside CsvMapped instance
            hdrstring, np = '', 0
            while not hdrstring:
                # go find headerstring on top of selected file
                hdrstring = xmarks.construct_header(fpath, online=np)
                np += 1
                if np > 60:
                    print("Breaking out due to a lot of blank lines instead of headers")
                    break

            # look up header in database find import map
            importmap = hdrs.find_one(hdrstring)
            print('importmap? (keyed by line #{}: {}): '.format(np, hdrstring))
            pprint(importmap)
            # if no import-map, create the import-map:
            if not importmap:
                print("...creating import map ")
                importmap = xmarks.headers_to_mongo(hdrs, hdrstring)
                if importmap:
                    print('hdrstring: {}'.format(hdrstring))
                    print('importmap ({} items):'.format(len(importmap)))
                    pprint(importmap)
                    hdrs.insert({hdrstring: importmap})
                    # need to include some rules about adding, subtracting quantities
            # using the import-map, import to db the csv data stored in the instance
            csvdocs = xmarks.parsedata(importmap, top_skip=np)
            # on success, add the input filename to database of imported_fn
            if csvdocs:
                print("---------  Actions against CSV-import-lines  --------------------------")
                actions = [u'Add/Subtract_Current_Quantities',
                           u'New_Data_Bulk_Insert',
                           u'Current_Price_Changes',
                           u'Update_and_Add_Data_(not_quantities)',
                           u'Skip_this_Import']
                actiondd = {a: t for a, t in enumerate(actions)}
                actnum, actiontype = selections(actiondd, prompt="Choose the type of action to use on the data: ")

                # divide items that are in database, items out
                
                # init bulk ops to insert many lines
                bulk = stuffdb.initialize_unordered_bulk_op()
                if actiontype == u'Add/Subtract_Current_Quantities':
                    for addline in csvdocs:
                        if u'barcode' in importmap:
                            bulk.find(addline[u'barcode'])

                WORKING = True
                while WORKING:
                    try:
                        bulk.execute()
                        WORKING = False
                    except BulkWriteError as bwe:
                        WORKING = True
                        pprint(bwe.details)




            # record the usage of the csv-file in the database along with action taken
            importedfn.insert({xmarks.fn_ctime(fpath): actiontype})
        # assign correct info to correct keys for insertion into current db collection
        addlist = []
    """
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

    print('Goodbye!')
    exit(0)

