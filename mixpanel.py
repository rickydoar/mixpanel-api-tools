import hashlib
import ast
import urllib
import pickle
import time
import math
import csv as csv1
try:
    import json
except ImportError:
    import simplejson as json
from random import randint


class Mixpanel(object):

    FORMAT_ENDPOINT = 'http://mixpanel.com/api'
    RAW_ENDPOINT = 'http://data.mixpanel.com/api'
    VERSION = '2.0'

    def __init__(self, api_key, api_secret, token=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.token = token
        
    def request(self, methods, params, debug_request=0):
        """
            methods - List of methods to be joined, e.g. ['events', 'properties', 'values']
                      will give us http://mixpanel.com/api/2.0/events/properties/values/
            params - Extra parameters associated with method
        """
        params['api_key'] = self.api_key
        params['expire'] = int(time.time()) + 600   # Grant this request 10 minutes.

        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)

        if 'export' in methods:
            ENDPOINT = self.RAW_ENDPOINT
        else:
            ENDPOINT = self.FORMAT_ENDPOINT

        request_url = '/'.join([ENDPOINT, str(self.VERSION)] + methods) + '/?' + self.unicode_urlencode(params)

        if debug_request == 1:
            print request_url
        
        request = urllib.urlopen(request_url)
        data = request.read()

        return data

    def unicode_urlencode(self, params):
        """
            Convert lists to JSON encoded strings, and correctly handle any 
            unicode URL parameters.
        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list): 
                params[i] = (param[0], json.dumps(param[1]),)

        return urllib.urlencode(
            [(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
        )

    def hash_args(self, args, secret=None):
        """
            Hashes arguments by joining key=value pairs, appending a secret, and 
            then taking the MD5 hex digest.
        """
        for a in args:
            if isinstance(args[a], list): args[a] = json.dumps(args[a])

        args_joined = ''
        for a in sorted(args.keys()):
            if isinstance(a, unicode):
                args_joined += a.encode('utf-8')
            else:
                args_joined += str(a)

            args_joined += '='

            if isinstance(args[a], unicode):
                args_joined += args[a].encode('utf-8')
            else:
                args_joined += str(args[a])

        hash = hashlib.md5(args_joined)

        if secret:
            hash.update(secret)
        elif self.api_secret:
            hash.update(self.api_secret)
        return hash.hexdigest()

    def event_export(self, params, debug=0):
        if debug == 1:
            data = api.request(['export'], params, debug)
        data = self.request(['export'], params)
        data = data.split("\n")[:-1]
        json_data = []
        for event in data:
            json_data.append(json.loads(event))
        return json_data

    def formatted_event_export(self, endpoint, params, debug=0):
        data = self.request([endpoint], params, debug)
        data = json.loads(data)
        return data

    def segmentation(self, params = None, debug = 0):
        required_params = ['to_date', 'from_date', 'event']
        optional_params = ['on', 'unit', 'where', 'limit', 'type']
        endpoint = 'segmentation'
        return self.validator(params, required_params, optional_params, endpoint, debug)

    def events(self, params = None, debug = 0):
        required_params = ['event', 'type', 'unit', 'interval']
        optional_params = ['format']
        endpoint = 'events'
        return self.validator(params, required_params, optional_params, endpoint, debug)


    def events_top(self, params = None, debug = 0):
        required_params = ['type']
        optional_params = ['limit']
        endpoint = 'events/top'
        return self.validator(params, required_params, optional_params, endpoint, debug)

    def arb_funnels(self, params = None, debug = 0):
        required_params = ['events', 'from_date', 'to_date']
        optional_params = ['interval', 'length', 'on']
        endpoint = 'arb_funnels'
        return self.validator(params, required_params, optional_params, endpoint, debug)

    def retention(self, params = None, debug = 0):
        required_params = ['from_date', 'to_date', 'retention type defaults to birth']
        optional_params = ['retention_type', 'born_event', 'event', 'born_where', 'where', 'interval', 'interval_count', 'unit', 'limit', 'on']
        endpoint = 'retention'
        return self.validator(params, required_params, optional_params, endpoint, debug)

        
    def validator(self, params, required, optional, endpoint, debug):
        validated = True
        if params == None:
            return "Required Params: %s, Optional Params: %s" % (required, optional)
        for param in required:
            if param not in params:
                print 'You must add the parameter "%s" to the params as it is a required parameter of this endpoint' % (param)
                validated = False
        for param in params:
            if param not in required and  param not in optional:
                print '%s is not a recognized parameter of this endpoint' % (param)
                validated = False
        if validated:
            data = self.formatted_event_export(endpoint, params, debug)
            return data
        else:
            return 'Please reformat your request and try again.'

def csv(data):
    if type(data) == type([1,2,3]) and 'event' in data[0]:
        f = csv1.writer(open("raw_data%s%s.csv" % (int(math.floor(time.time())), randint(1,100)), "wb+"))
        keys = ['event']
        for event in data:
            for prop in event['properties']:
                if prop not in keys:
                    keys.append(prop)
        f.writerow(keys)
        keys.remove('event')
        for event in data:
            line = [event['event']]
            for key in keys:
                if event['properties'].get(key):
                    line.append(str(event['properties'][key]))
                else:
                    line.append('')
            try:
                f.writerow(line)
            except:
                temp = []
                for l in line:
                    temp.append(l.encode('utf-8'))
                f.writerow(temp)
        print "raw_data%s%s.csv" % (int(math.floor(time.time())), randint(1,100))
    elif data['data'].get('values'):
        f = csv1.writer(open("segmentation_data%s%s.csv" % (int(math.floor(time.time())), randint(1,100)), "wb+"))
        keys = ['event']
        for date in data['data']['series']:
            keys.append(date)
        f.writerow(keys)
        for segment in data['data']['values']:
            values = [segment]
            for date in data['data']['values'][segment]:
                values.append(data['data']['values'][segment][date])
            f.writerow(values)
        print "segmentation_data%s%s.csv" % (int(math.floor(time.time())), randint(1,100))







