import hashlib
import ast
import urllib
import pickle
import time
import math
import csv as csv1
from random import randint
try:
    import json
except ImportError:
    import simplejson as json
import base64


class Mixpanel(object):

    FORMAT_ENDPOINT = 'http://mixpanel.com/api'
    RAW_ENDPOINT = 'http://data.mixpanel.com/api'
    VERSION = '2.0'

    def __init__(self, api_key, api_secret, token=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.token = token
        
    def request(self, methods, params, debug=0, high_volume=0):
        params['api_key'] = self.api_key
        params['expire'] = int(time.time()) + 600   # Grant this request 10 minutes.

        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)

        if 'export' in methods:
            ENDPOINT = self.RAW_ENDPOINT
        else:
            ENDPOINT = self.FORMAT_ENDPOINT

        request_url = '/'.join([ENDPOINT, str(self.VERSION)] + methods) + '/?' + self.unicode_urlencode(params)

        if debug == 1:
            print request_url

        if high_volume == 1:
            output_file = "%s_data%s.txt" % (methods[0], int(math.floor(time.time())))
            request = urllib.urlretrieve(request_url, output_file)
            print output_file
        else:
            request = urllib.urlopen(request_url)
            data = request.read()
            return data

    def unicode_urlencode(self, params):
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list): 
                params[i] = (param[0], json.dumps(param[1]),)

        return urllib.urlencode(
            [(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
        )

    def hash_args(self, args, secret=None):
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

    def propset(self, distinct_id, params, operation):
        print distinct_id
        properties = {
            '$token': self.token,
            '$distinct_id': distinct_id,
            '$ignore_time':'True',
            '$ignore_alias':'True',
            operation:params   
        }
        data = base64.b64encode(json.dumps(properties))
        host = 'api.mixpanel.com'
        params = {
            'data': data,
            'verbose': 1,
            'ip':0,
        }
        print data
        url = 'http://%s/%s/?%s' % (host, 'engage', urllib.urlencode(params))
        response = json.load(urllib2.urlopen(url))
        if response['status'] != 1:
            raise RuntimeError('%s\n%s' % (url, response))

    def delete_people(self, params={}):
        self.people_export(params, debug=0, high_volume=1)
        GREENLIGHT = raw_input("This will delete %s users. File backup named %s  Enter 'YES' if this is correct:  " % (global_total, fname))
        if GREENLIGHT == 'YES':
            params = {'$delete':True}
            with open(fname,'r') as f:
                users = f.readlines()
            counter = len(users) // 100
            while len(users):
                batch = users[:50]
                self.update(batch, params)
                if len(users) // 100 != counter:
                    counter = len(users) // 100
                    print "%d bad users left!" % len(users)
                users = users[50:]

    def update(self, userlist, uparams):
        url = "http://api.mixpanel.com/engage/"
        batch = []

        for user in userlist:
            distinctid = json.loads(user)['$distinct_id']
            print distinctid
            tempparams = {
                    'token':self.token,
                    '$distinct_id':distinctid,
                    '$ignore_alias':'True'
                    }
            tempparams.update(uparams)
            batch.append(tempparams)

        payload = self.unicode_urlencode({"data":base64.b64encode(json.dumps(batch)), "verbose":1})
        request_url = '%s?%s' % (url, payload)
        request = urllib.urlopen(request_url)
        message = request.read()

        if json.loads(message)['status'] != 1:
            print message
            print request_url


    def people_export(self, params={}, debug=0, high_volume=0):
        response = self.request(['engage'], params, debug)
        params.update({
                    'session_id' : json.loads(response)['session_id'],
                    'page':0
                    })
        global global_total
        global_total = json.loads(response)['total']
        print "Session id is %s \n" % params['session_id']
        print "Here are the # of people %d" % global_total
        global fname
        fname = "backup-people%s%s.txt" % (int(math.floor(time.time())), randint(1,100))
        has_results = True
        total = 0

        if high_volume == 1:
            f = open(fname, 'w')
        else:
            data = []
        while has_results:
            responser = json.loads(response)['results']
            total += len(responser)
            has_results = len(responser) == 1000
            for people in responser:
                if high_volume == 1:
                    f.write(json.dumps(people)+'\n')
                else:
                    data.append(json.dumps(people))
            print "%d / %d" % (total,global_total)
            params['page'] += 1
            if has_results:
                response = self.request(['engage'], params)
        if high_volume == 1:
            print "File %s created" % (fname)
        else:
            json_data = []
            for people in data:
                json_data.append(json.loads(people))
            return json_data

    def event_export(self, params, debug=0, high_volume=0):
        if 'event' in params and isinstance(params.get('event'), str):
            return 'Event param must be in list format'
        if high_volume == 0:
            data = self.request(['export'], params, debug, high_volume)
            data = data.split("\n")[:-1]
            json_data = []
            for event in data:
                json_data.append(json.loads(event))
            return json_data
        else:
            self.request(['export'], params, debug, high_volume)


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
    if isinstance(data, str) or data is None:
        print "csv expects JSON data"

    elif isinstance(data, list):
        if data[0].get('event'):
            '''Raw Data'''
            fname = "raw_data%s%s.csv" % (int(math.floor(time.time())), randint(1,100))
            f = csv1.writer(open(fname, "wb+"))
            keys = ['event']
            for event in data:
                for prop in event['properties']:
                    if prop not in keys:
                        keys.append(str(prop).encode('utf-8'))
            f.writerow(keys)
            keys.remove('event')
            for event in data:
                values = [event['event']]
                for prop in keys:
                    if event['properties'].get(prop):
                        if isinstance(event['properties'][prop], int) or isinstance(event['properties'][prop], list) or isinstance(event['properties'][prop], float):
                            values.append(str(event['properties'][prop]))
                        else:
                            values.append(event['properties'][prop].encode('utf-8'))
                    else:
                        values.append('')
                try:
                    f.writerow(values)
                except:
                    print values
            print fname

        elif data[0].get('$distinct_id') and data[0].get('$properties'):
            '''People Data'''
            fname = "people_data%s%s.csv" % (int(math.floor(time.time())), randint(1,100))
            f = csv1.writer(open(fname, "wb+"))
            keys = ['distinct_id']
            for people in data:
                for prop in people['$properties']:
                    if prop not in keys:
                        keys.append(str(prop).encode('utf-8'))
            f.writerow(keys)
            keys.remove('distinct_id')
            x = 0
            for people in data:
                values = [people['$distinct_id']]
                for prop in keys:
                    if people['$properties'].get(prop):
                        if isinstance(people['$properties'][prop], int) or isinstance(people['$properties'][prop], list) or isinstance(people['$properties'][prop], float):
                            values.append(str(people['$properties'][prop]))
                        else:
                            values.append(people['$properties'][prop].encode('utf-8'))
                    else:
                        values.append('')
                try:
                    f.writerow(values)
                except:
                    print values
            print fname

    elif data['data'].get('values') and data['data'].get('series'):
        '''Segmentation Data'''
        fname = "segmentation_data%s%s.csv" % (int(math.floor(time.time())), randint(1,100))
        f = csv1.writer(open(fname, "wb+"))
        if len(data['data']['values']) == 1:
            keys = ['Event']
        else:
            keys = ['Property']
        for date in data['data']['series']:
            keys.append(date)
        f.writerow(keys)
        for segment in data['data']['values']:
            if isinstance(segment, int) or isinstance(segment, list) or isinstance(segment, float):
                values = [str(segment)]
            else:
                values = [segment.encode('utf-8')]
            for date in data['data']['values'][segment]:
                values.append(data['data']['values'][segment][date])
            f.writerow(values)
        print fname





