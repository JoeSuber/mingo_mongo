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
import pymongo
from pymongo import MongoClient
#
from pymongo.errors import DuplicateKeyError, PyMongoError
from pymongo import ASCENDING, DESCENDING
import os
from pprint import pprint
import cPickle

# A problem: data from several sources (with different labels) should affect the same objects.
# when presented with data to be imported into a given database
# -1: has this particular file already been imported? if so warn & exit
# 0: check if these steps have been pre-defined for the file's kind of head-line
# 1: determine which collection the data is going to reside in
#   a) an auto-match of column-headers against each collection's
#       document fields could do this but only for the exact
#       same kind of input files. With a few headers missing from the incoming bunch we could
#       still show an exact match of just those, but be totally wrong.
# 2: If the data is from a 'virgin' file-header-type, the user must determine the mapping of
#   column-header to db-collection-field and actions. Perhaps some automation with exact string-matches.
# 2.5: User could always choose the operation(s), ie, adding quantity, adding all-new, price changes, etc.
# 3: Must determine the 'key' values that are unique among imports and that this key-column maps
#       to equivalent field in destination collection.
# 4: Ensure that an import has required columns for the planned operation
# 5:


actions = {0: "Import new data / inventory (don't write-over old)",
           1: "Import Counted-Inventory info - only apply to current items, no adding sku",
           2: "Check Items Against Inventory, creating two lists, Ins and Outs",
           }

def selections(dd=None, prompt='Choose from above'):
    """
    given dict of numbered options like {1: 'choose me', ...}
    present user with options and return the chosen integer key and value string
    """
    choice = 1
    if not dd:
        print('selections: empty choice dictionary, returning 0')
        return 0, None
    for choice, dbnm in dd.viewitems():
        print('{:4}- {}'.format(choice + 1, dbnm))
    q = -1000
    while (q - 1) not in dd.keys():
        if len(dd) > 1:
            q = int(unicode(raw_input(prompt)))
        else:
            print('only one choice.. ')
            q = 1
    return q - 1, dd[q - 1]


def createdbnames(dbd=None):
    """
    my pie-in-the-sky plans to automate all the drudge and operate using real data
    (lists are to be converted to keys of dict with empty values when initializing db)
    """
    if not dbd:
        dbd = {u'manufacturer': {u'3letter_code': u'GAW',  u'Full_Name': u'Games_Workshop', u'Contact_Phone': u'',
                                 u'Contact_email': u'', u'Contact_Web': u'',
                                 u'Sales_Rep': u'Ben Cumming', u'My_Account': 0},
               u'my_customers': [u'my_cust_id', u'email', u'requested_this', u'pre_paid_for', u'credit_file',
                                 u'face_rec', u'purchased', u'returned', u'ebay_notes', u'paid_on_invoice',
                                 u'shipped_out_date', u'ship_tracked', u'ship_rcvd', u'notes'],
               u'my_interns': [u'contact', u'email', u'schedule_past', u'schedule_future', u'good_things', u'bad_things'],
               u'store_inventory': [u'description', u'we_sell_price', u'multipack_sku',
                                    u'multipack_quant', u'multiprice', u'manufacturer', u'barcode', u'we_buy_cost',
                                    u'date_modified', u'quant_on_invoice', u'sku_for1', u'mfr_3letter', u'sku_alliance',
                                    u'sku_alt', u'increment_quant', u'decrement_quant', u'current_whole_quant',
                                    u'desired_quant', u'notes'],
               u'stocking': [u'string_id', u'description', u'we_bought_history',
                             u'we_sold_history', u'date_added_toinv', u'prefer_dist_list', u'quant_want_min',
                             u'quant_want_max', u'quant_on_reorder', u'dist_alerts', u'velocity'],
               u'index_keys': {u'manufacturer': [(u'3letter_code', 1)],
                               u'my_customers': [(u'my_cust_id', 1), (u'email', 1)],
                               u'my_interns': [(u'email', 1)],
                               u'store_inventory': [(u'sku_for1', 1), (u'barcode', 1), (u'sku_alt', 1)],
                               u'stocking': [(u'string_id', 1)],
                               u'import_directions': [(u'filepath', 1)],
                               u'index_keys': [(u'index_keys', 1)],
                               u'commandd': [(u'CUR', 1)]},
               u'import_directions': {u'headline': u'this|!is_not|!a_real|!header|!string',
                                      u'filepath': u'/some/once/valid/file_path.csv|23423234.23',
                                      u'special_commands': u'0,NUMB,3,NO',
                                      u'csv_to_db': {u'this': u'barcode',
                                                     u'is_not': u'description',
                                                     u'a_real': u'quant_on_invoice',
                                                     u'string': u'we_sell_price'}},
               u'commandd': {u'CUR': u'Column is in currency with decimal - Convert to Cents - (Implies NUMB)',
                             u'NUMB': u"Number-Strings into Integers (leave off decimal point)",
                             u"NEW": u"Import Only if New or Blank in Existing Data",
                             u"REPLACE": u"Remove Existing Items that Have this Key (implies KEY)",
                             u"KEY": u"This Column Holds a 'Key' Lookup Value ",
                             u"ADD": u"Increment or Decrement (an existing Integer value) by This Much",
                             u"NO": u"Don't Use This Imported Column to Change Anything"}
               }
        return dbd

def de_string(i, isint=True):
    """ if getting a string, return pennies on the dollar, or stripped-down string"""
    if isinstance(i, (str, unicode)):
        try:
            if isint:
                return int(i.replace(".", "").replace("$", "").strip())
            else:
                return float(i.replace("$", "").strip())
        except ValueError:
            return i.strip()
    return i

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
        if not usedb_name:
            usedb_name = u"31cent"
        primarydb = client[usedb_name]
        primaries = {num: unicode(pri) for num, pri in enumerate(primarydb.collection_names())}
        key, collection_name = selections(primaries, prompt='Choose the collection within {} to explore: '.format(usedb_name))
        print('okay - choice was {:3} - {} \n'.format(key+1, collection_name))
        if not collection_name:
            collection_name = u"fake"
        thestuffdb = primarydb[collection_name]
        print("This collection contains {} items ".format(thestuffdb.count()))
        end_dd = {0: ' done exploring - move on to import, etc',
                  1: ' continue exploration',
                  2: " print out collection '{}' aka {}".format(collection_name, thestuffdb)}
        key, desire = selections(end_dd, prompt='sitting on {}.{} - Choose from : '.format(usedb_name, collection_name))
        print('okay - choice was {:3} - {} \n'.format(key+1, desire))
        done = not key
        if key > 0:
            done = 0
            if key == 2:
                for plc, item in enumerate(thestuffdb.find()):
                    print('#{:6}: {}'.format(plc, item))

    print(' @!@!@!@!    Active mongo parts: {}.{}    !@!@!@!@\n'.format(usedb_name, collection_name))
    return primarydb, thestuffdb


class CsvMapped(dict):
    """
    Provides persistent pairing of parsed page parts particularly to Mongodb
    collection dicts. Mappings can be defined here and also load, if available,
    mappings created earlier during interactive matching process. Goal is
    to map a csv-source file to database once & from that time forward, have
    similarly headed documents automatically recognized & imported.
    """
    def __init__(self, startlooking='Desktop', cmd=u'commandd'):
        self.fpath = ''
        self.thetext = ''
        self.spltr = '|!'
        self.defmark = u'(@)'
        self.comma = ','
        self.startlooking = startlooking
        self.catlist = createdbnames()[u'store_inventory']
        self.cmd = cmd

    def fn_ctime(self, fn):
        """
        attach os-provided last-modified-time to filename in a consistent way
        """
        return unicode(fn + self.spltr + unicode(os.stat(fn).st_mtime))

    def csvsources(self, usedcsvdb, startdir=None, looking_for='.csv'):
        """
        gather and track the input data (received via e.g. csv format) for
        later import into the mongodb. Return only valid choices as dict.
        """
        if looking_for:
            self.looking_for = looking_for
        if not startdir:
            startdir = os.path.join(os.path.expanduser('~'), self.startlooking)
        print("started looking for *{} from {}".format(looking_for, startdir))

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
            print("DEBUG (should be string) 'checking' = {}".format(checking))
            # must find appropriate place to add the chosen filename to database
            if checking[u'filepath'] in examining:
                deadnum = longfnkey[checking[u'filepath']]
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

    def headers_to_mongo(self, db, hstrip, catchoice=None):
        """
        Interactively generates the import-map and special instructions
        to be saved and later referenced by the header-string.
        """
        if not catchoice:
            # enumerated list of possible fields defaults to inventory style header-choices
            catchoice = {kk: vv for kk, vv in enumerate(self.catlist)}
        # enumerated list of column headers
        hdrlist = {kk: vv.strip() for kk, vv in enumerate(hstrip.split(self.spltr))}
        catstart = len(catchoice)
        catchoice.update({len(catchoice): ' - NOT USED - ', len(catchoice)+1: ' - START OVER - '})
        pprint(catchoice)
        catcutoff = len(catchoice) - catstart
        genmap = {}
        hdr_key_list = []
        total = len(hdrlist)
        for togo, (dbhdrkey, dbhdr) in enumerate(hdrlist.viewitems()):
            print('.............  {} .........................................'.format(total - togo))
            print('of {} columns in csv-file, we still must assign {} a place'.format(total, total - togo))
            selnum, selcategory = selections(catchoice,
                                             prompt='- HEADER MAP - Select Match for |{}| : '.format(dbhdr))
            print('Your Choice: #{} - {}'.format(selnum, selcategory))
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

        # finish out map with optional user-defined uniform default values for blanks in column
        if len(catchoice):
            print(" Press <Enter> for each header if you wish its values to default to 'None'")
            for leftnum, leftover in catchoice.viewitems():
                if leftnum < (len(catchoice) - catcutoff):
                    genmap[leftover] = self.defmark + unicode(raw_input(
                        " ALL '{}' will have a value = : ".format(leftover))).decode()

        pprint(createdbnames()[self.cmd])
        #pprint(genmap)

        return genmap

    def ask_where_join(self, lpl):
        """
        The rare and unfortunate result of stray line-splitters surrounded by extra
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
        # get rid of double-quotes entirely
        line = line.replace('""', '')
        if '"' in line:
            h, q, t = line.partition('"')
            if '"' in t:
                h2, q, t2 = t.partition('"')
                h2 = h2.replace(self.comma, " ")
                return "".join([h, q, h2, q, t2])
        return line

    def parsedata(self, headers, headline, top_skip=0):
        """
        the previously read file is a list of lines stored in the instance. The lines
        are commonly split out by comma or other delimiter, then assigned to dict
        """
        extra_defs = []
        string_ordered = []
        # put the correct db-equivalent above the incoming columns
        for column in headline.split(self.spltr):
            for kk, vv in headers.viewitems():
                if column == vv:
                    string_ordered.append(kk)
        try:
            permits = headers.pop(u'_id')
        except Exception as e:
            print("passed in headers probably lacked '_id' ")
            print("but they were cute so I let them in anyway... Error = {}".format(e))
        # set up the values that are defaults for every line
        for kky, val in headers.viewitems():
            if isinstance(val, unicode) and (self.defmark in val):
                val = val.replace(self.defmark, " ")
                if val == "":
                    pass
                else:
                    extra_defs.append(de_string(val))
                    string_ordered.append(kky)
                    #print "parsedata says extra val is: ", val

        header_quant = len(headers)
        numer = 0
        csvdocs = []
        #iterate over the full lines
        for numer, csvline in enumerate(self.thetext):
            # often skipping line zero as it should be the header-line
            if numer >= top_skip:
                # fix some trip-ups in quoted data fields
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
                line = {h: de_string(cell.strip()) for h, cell in zip(string_ordered, lineparts)}
                # pprint(line)
                csvdocs.append(line)
        commands = createdbnames()[u'commandd'].keys()
                #u'commandd' = {u'CUR': u'Column is in currency with decimal - Convert to Cents - (Implies NUMB)',
                #             u'CENT': u"Convert & Treat value like whole Cents (Implies NUMB)",
                #             u'NUMB': u"Turn Number-Strings into counting Integers",
                #             u"NEW": u"Import Only if New or Blank in Existing Data",
                #             u"REPLACE": u"Remove Existing Items that Have this Key (implies KEY)",
                #             u"KEY": u"This Column Holds a 'Key' Lookup Value ",
                #             u"ADD": u"Increment or Decrement (an existing Integer value) by This Much",
                #             u"NO": u"Don't Use This Imported Column to Change Anything"}

        print(' {} items are to be gleaned from this CSV file: {} '.format(numer, self.fpath))
        return csvdocs


if __name__ == "__main__":
    #dbserverip = '192.168.0.105'
    dbserverip = 'localhost'
    dbserverport = 27017
    xmarks = CsvMapped()
    # max_np is the number of 'readline()' lines to scan before giving up
    max_np = 10
    # some 'databases' and 'collections' in the MongoClient
    client = MongoClient(dbserverip, dbserverport)
    #currentdb = client.database_names()
    currentdbchoices = {num: dbname for num, dbname in enumerate(client.database_names())}
    currentdbchoices.update({len(currentdbchoices): 'Name A New Main Database'})
    _, choice = selections(currentdbchoices, prompt="Select :")
    while (choice == 'Name A New Main Database') or (choice is None):
        choice = unicode(raw_input(u"Type in the new Database name :"))
        if (u"." in choice) or (u"'" in choice) or (u"\\" in choice) or (u'"' in choice):
            print(u"Don't use those funny characters \n  Try Again \n")
            choice = None

    # dbmap is the hard-coded preliminary idea of the db structure as a dict
    dbmap = createdbnames()
    col = ()
    dbb = client[choice]
    for dbnm, dbvals in dbmap.viewitems():
        try:
            dbb[dbnm].create_index(dbmap[u'index_keys'][dbnm], unique=True, dropDups=True)
        except DuplicateKeyError, ValueError:
            print("no indexing could be completed on {}".format(dbnm))
        if isinstance(dbvals, list):
            dbvals = {asis: "" for asis in dbvals}
        try:
            col = dbb[dbnm].insert(dbvals)
        except DuplicateKeyError:
            print("No Duplication allowed for {}".format(dbvals))
        print("attempted adding a collection like: {} ".format(dbb[dbnm]))
        print("it contains:")
        pprint([item for item in dbb[dbnm].find()])
    print("created / verified databases named: ")
    print(client.database_names())

    # user chooses the database and collection we are messing with:
    overalldb, stuffdb = explore(0, client)

    # some collections to load up:
    hdrs = overalldb[u'import_directions']

    # find database collection labels for 'explored' stuffdb
    labelset = set()
    for hp in stuffdb.find():
        labelset = set(kk[0] for kk in hp.viewitems())
    print('labelset (from things already in db): ')
    pprint(labelset)

    # validate found source files for potential input into db
    new_fn_dd = xmarks.csvsources(hdrs, startdir=None, looking_for='.csv')

    # now have user choose one source-file until none left or done:
    selnum = 0
    while not selnum:
        selnum, fpath = selections(new_fn_dd, prompt='Above are saved files you can add to mongod. Choose wisely: ')
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
                if np > max_np:
                    print("Search contents of file: {}".format(fpath))
                    print("Stopped due to {} blank lines on top instead of column-headers".format(np))
                    break

            # look up header in database find import map
            print("Looking in {}".format(hdrs))
            print("for previous imports using: ")
            print("'{}'\n".format(hdrstring))
            importmap = hdrs.find_one({u'headline': hdrstring})
            if importmap:
                minimap = importmap[u'csv_to_db']
                print('importmap? (keyed by line #{}: {}): '.format(np, hdrstring))
                pprint(importmap)
                print("^^^^^^^^^^^ total: {} columns ^^^^^^^^^^^".format(len(minimap)))
                yesno = {0: "Use this map & directions to import the file to database",
                         1: "Throw it out and create new mapping"}
                selnum, _ = selections(yesno, prompt="Select : ")
                print(" your choice: {}".format(yesno[selnum]))
                if selnum:
                    hdrs.remove({u'headline': hdrstring})
                    importmap = None
                    importdirections = None

            # if no import-map, create the import-map:
            if not importmap:
                print("...creating import map ")
                importmap = xmarks.headers_to_mongo(hdrs, hdrstring)
                if importmap:
                    print('hdrstring: {}'.format(hdrstring))
                    print('importmap ({} items):'.format(len(importmap)))
                    pprint(importmap)

            # using the import-map, dictify data stored in the instance
            csvdocs = xmarks.parsedata(importmap, hdrstring, top_skip=np)

            if csvdocs:
                # the bulk-op:
                finished = 0
                for finished, doc in enumerate(csvdocs):
                    try:
                        stuffdb.insert(doc)
                    except pymongo.errors.DuplicateKeyError as dup:
                        print('##########  {}  #######'.format(finished))
                        print(dup)
                print("finished = {}".format(finished))
                # on success, add the input filename to database of imported_fn
                # save the new map in database because it has succeeded in making a csvdoc
                hdrs.insert({u'headline': hdrstring})
                hdrs.insert({u'filepath': xmarks.fn_ctime(fpath)})
                hdrs.insert({u'special_commands': None})
                hdrs.insert({u'for_collection': unicode(stuffdb)})
                hdrs.insert({u'csv_to_db': importmap})
                print("---------  Actions against CSV-import-lines  --------------------------")

                # divide items that are in database, items out
"""
                # init bulk ops to insert many lines
                bulk = overalldb.initialize_unordered_bulk_op()
                if actiontype == u'New_Data_Bulk_Insert':
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
            #importedfn.insert({u'fn': xmarks.fn_ctime(fpath)})
        # assign correct info to correct keys for insertion into current db collection
        #addlist = []

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

    #print('Goodbye!')
   # exit(0)

