import es
import datetime
import urllib2
import json
import utils
import worker
import threading
import internetarchive
import pdb
import time

config = None
logger = None

def change_to_dates(identifier, item, date_keys):
    for key in date_keys:
        try:
            tmp_date = item[key]
            if isinstance(tmp_date, str) or isinstance(tmp_date, unicode):
                item[key] = datetime.date(int(tmp_date[0:4]), int(tmp_date[4:6]), int(tmp_date[6:8]))
        except:
            logger.exeception("can't change %s['%s'] to date" % (identifier, key))
    return


# make sure this is threadsafe
def get_metadata_and_add_to_items(identifier, action, doc, items, item_lock):
    logger.info("getting metadata for %s" % identifier)
    metadata = internetarchive.get_item(identifier).metadata
    for key in ['republisher_date', 'scandate']:
        try:
            m_date = metadata[key]
            doc[key] =  datetime.date(int(m_date[0:4]), int(m_date[4:6]), int(m_date[6:8]))
        except KeyError:
            logger.error('could not find %s in metadata for %s' % (key, identifier))
    # ok, we have the action, now let's add it to the items list
    item_lock.acquire()
    items.append(action)
    item_lock.release()
    return
    

# process the json from one scan report
def add_or_update_these_books(book_records, previous_book_data, index, items):
    lock = threading.Lock()
    count = 0
    added = 0
    updated = 0
    for item in book_records:
        action = None
        identifier = item['identifier']
        i_updated = item['updated']
        (id, schema, doc) = previous_book_data.get(identifier, (None, None, None))
        if id:
            # we only update if the updated field has changed
            if i_updated != doc['updated']:
                updated += 1
                logger.info('updating %, was last updated at %s, most recent scan showed %s' % (identifier, existing_doc['updated'], updated))
                i_copy = item.copy()
                action = {'_type' : schema, '_index': index, '_op_type' : 'update', '_id': id, 'doc': i_copy}
                serial_number = int(doc['serial_number'])
                item['serial_number'] = serial_number + 1
        else:
            # new book we should add it
            added += 1
            item.update({'_type' : 'project', '_index': index})
            item['serial_number'] = 0
            i_copy = item.copy()
            action = i_copy
            logger.info("new book %s found in scan data, adding it" % identifier)
        # schedule it to be added
        if action:
            worker.run_in_worker(get_metadata_and_add_to_items, identifier, action, i_copy, items, lock)
        count += 1
    return added, updated
            
        

def load_from_scan_report(previous_book_data):
    dt = datetime.datetime.now()
    items = []
    total_added = 0
    total_updated = 0
    for index in range(int(config.get('books', 'weeks'))):
        year, iso_week, iso_weekday = dt.isocalendar()
        url = "https://books-general.archive.org/scan_reports/books_items_%04d_%02d.json" % (year, iso_week)
        logger.info('loading from %s' % url)
        try:
            data = json.loads(urllib2.urlopen(url).read())
            added, updated = add_or_update_these_books(data, previous_book_data, config.get('es','index'), items)
            total_added += added
            total_updated += updated
        except:
            logger.exception('missing book items file: %s' % url)
        dt -= datetime.timedelta(days=7)
    logger.info("%d added, %d updated... waiting to finish" % (total_added, total_updated))
    try:
        while True:
            if 0 == worker.current_workers:
                break;
            else:
                logger.info('waiting for %d workers to finish' % worker.current_workers)
            time.sleep(5)
        #worker.work_queue.join()
    except KeyboardInterrupt:
        pdb.set_trace()
    return items


def load_books_from_es(es):
    data = {}
    for id, d_type, doc in es.map_over_data('_type:project', source = ['identifier', 'updated', 'serial_number']):
        data[doc['identifier']] = [id, d_type, doc]
    return data

        
def update_projects():
    global config, logger
    config = utils.get_config("config.txt")
    logger = utils.setup_logging(config, __name__)
    worker.setup(100,logger)
    conn = es.ESConnector(config, logger)
    books = load_books_from_es(conn)
    items = load_from_scan_report(books)
    conn.bulk(items)
    return

if __name__ == "__main__":
    update_projects()
