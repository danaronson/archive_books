from elasticsearch import Elasticsearch, helpers, serializer, compat, exceptions
import logging
import ConfigParser
import os
import sys
import json

# see https://github.com/elastic/elasticsearch-py/issues/374
class JSONSerializerPython2(serializer.JSONSerializer):
    """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
    See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
    A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
    as ascii can be found here: https://docs.python.org/2/library/json.html#basic-usage
    """
    
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)


class ESConnector():
    LOG_LEVELS = {"CRITICAL":logging.CRITICAL,"ERROR":logging.ERROR,"WARNING":logging.WARNING,"INFO":logging.INFO,"DEBUG":logging.DEBUG,"NOTSET":logging.NOTSET}

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

        self.debug = 'true' == self.config.get('default', 'debug').lower()

        self.es = Elasticsearch([self.config.get('es', 'host')], 
                         port=int(self.config.get('es', 'port')), use_ssl=('True' == self.config.get('es','use_ssl')),
                         url_prefix = self.config.get('es', 'url_prefix'), serializer=JSONSerializerPython2(),
                         timeout=100)

    # using scrolling, map over a query
    def map_over_data(self, query, size=1000, source=True):
        index = self.config.get('es', 'index')
        query = {  "query": {    "bool": {      "must": [        {          "query_string": {            "analyze_wildcard": True,            "query": query}}]}}}
        self.logger.debug("Querying ES for: '%s'" % query)
        try:
            page = self.es.search(index=index, body=query,scroll='1h',size=size, _source=source)
        except Exception as e:
            self.logger.exception("got exception while trying to search index: %s with query '%s'" % (index, query))
            raise e
        self.logger.debug('got results');
        reported_size =  page['hits']['total']
        count = 0
        while True:
            if 0 == len(page['hits']['hits']):
                break
            for res in page['hits']['hits']:
                source = res.get('_source', False)
                yield res['_id'], res['_type'], source
                count += 1
            if count == reported_size:
                break
            self.logger.debug("scrolling search results: '%s'" % query)
            try:
                page = self.es.scroll(scroll_id = page['_scroll_id'], scroll = '1h')
            except Exception as e:
                self.logger.exception("got exception while trying to scroll index: %s with query '%s'" % (index, query))
                raise e
            finally:
                self.logger.debug("got scroll results: '%s'" % query)

    def bulk(self, items):
        if self.debug:
            self.logger.warning('in debug mode, not uploading to es')
            return []
        else:
            return helpers.bulk(self.es, items, chunk_size=5000)

    
