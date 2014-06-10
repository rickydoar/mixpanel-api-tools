import hashlib
import ast
import urllib
import pickle
import time
try:
    import json
except ImportError:
    import simplejson as json


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
		return data

	def formatted_event_export(self, endpoint, params, debug=0):
		if debug == 1:
			data = api.request(['export'], params, debug)
		data = self.request([endpoint], params)
		return data

	def segmentation(self, params = None):
		required_params = ['to_date', 'from_date', 'event']
		optional_params = ['on', 'unit', 'where', 'limit', 'type']
		endpoint = 'segmentation'
		if params == None:
			return "Required Params: %s, Optional Params: %s" % (required_params, optional_params)
		return self.validator(params, required_params, optional_params, endpoint)

	def events(self, params = None):
		required_params = ['event', 'type', 'unit', 'interval']
		optional_params = ['format']
		endpoint = 'events'
		if params == None:
			return "Required Params: %s, Optional Params: %s" % (required_params, optional_params)
		return self.validator(params, required_params, optional_params, endpoint)


	def events_top(self, params = None):
		required_params = ['type']
		optional_params = ['limit']
		endpoint = 'events/top'
		if params == None:
			return "Required Params: %s, Optional Params: %s" % (required_params, optional_params)
		return self.validator(params, required_params, optional_params, endpoint)

	def arb_funnels(self, params = None):
		required_params = ['events', 'from_date', 'to_date']
		optional_params = ['interval', 'length', 'on']
		endpoint = 'arb_funnels'
		if params == None:
			return "Required Params: %s, Optional Params: %s" % (required_params, optional_params)
		return self.validator(params, required_params, optional_params, endpoint)

		
	def validator(self, params, required, optional, endpoint):
		validated = True
		for param in required:
			if param not in params:
				print 'You must add the parameter "%s" to the params as it is a required parameter of this endpoint' % (param)
				validated = False
		for param in params:
			if param not in required and  param not in optional:
				print '%s is not a recognized parameter of this endpoint' % (param)
				validated = False
		if validated:
			data = self.formatted_event_export(endpoint, params)
			return data
		else:
			return 'Please reformat your request and try again'







