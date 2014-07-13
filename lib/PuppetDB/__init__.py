#!/bin/python -tt

import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
import json
import os
import re
import urllib, urllib2

from Utils.HTTPSClientAuthHandler import HTTPSClientAuthHandler
from Utils.Utils import first_usable_value

class PuppetDB():
    """ Common interface to the GSA PuppetDB
    """

    MATCH_OPERATORS = [u"=",u"~"]
    BINARY_OPERATORS = [u"=",u">",u"<",u">=",u"<=",u"~"]

    def __init__(self,
                 host=None,
                 cert=None,
                 key=None):
        """ Constructs a new PuppetDB interface object

            host - Hostname of the puppetdb instance to connect to
            cert - Path to client ssl certificate to use for auth. If blank will
                   fall back to PUPPETDB_CERT or '/mnt/live/deploy/infra/private/tools/puppetdb.crt'
            key  - Path to client ssl private key to use for auth. If blank will
                   fall back to PUPPETDB_KEY or '/mnt/live/deploy/infra/private/tools/puppetdb.crt'
        """
        self.host = first_usable_value([host, os.getenv(u'PUPPETDB_HOST')], u'puppetdb')
        self.cert = first_usable_value([cert, os.getenv(u'PUPPETDB_CERT')], u'%s/.phog/phog.crt' % os.getenv('HOME'))
        self.key  = first_usable_value([key,  os.getenv(u'PUPPETDB_KEY')],  u'%s/.phog/phog.key' % os.getenv('HOME'))

        self.opener = urllib2.build_opener(HTTPSClientAuthHandler(self.key, self.cert))

    def query(self, terminus, query):
        """ Queries arbitrary data from puppetdb
        """
        if query:
            url = u'https://%s:8081%s?%s' % (self.host, terminus, urllib.urlencode({u'query': query}))
        else:
            url = u'https://%s:8081%s' % (self.host, terminus)

        return json.loads(self.opener.open(url).read(), encoding='utf-8')

    # Terminus query wrappers

    def query_facts(self, query):
        """ Queries abitrary data from the puppetdb facts terminus
            See https://docs.puppetlabs.com/puppetdb/1.5/api/query/v3/facts.html
        """
        return self.query(u'/v3/facts/', query)

    def query_resources(self, query):
        """ Queries arbitrary data from the puppetdb resources terminus
            See https://docs.puppetlabs.com/puppetdb/1.5/api/query/v3/resources.html
        """
        return self.query(u'/v3/resources/', query)

    def query_nodes(self, query):
        """ Queries abitrary data from the puppetdb nodes terminus
            See https://docs.puppetlabs.com/puppetdb/1.5/api/query/v3/nodes.html
        """
        return self.query(u'/v3/nodes/', query)

    def query_node_facts(self, node, query):
        """ Queries arbitrary data from the puppetdb nodes/facts terminus
        """
        return self.query(u'/v3/nodes/%s/facts' % (node,), query)

    def query_node_resources(self, node, query):
        """ Queries arbitrary data from the puppetdb nodes/resources terminus
        """
        return self.query(u'/v3/nodes/%s/resources' % (node,), query)

    # Helper methods
    # These methods wrap the terminus queries above for the common use cases

    def get_node(self, name):
        """ Returns node information for a single node, including timestamps
            for data stored in the catalog
        """
        return self.get_nodes(name, operator=u'=')

    def get_nodes(self, name, operator="="):
        """ Return node information including timestamps for one or more nodes,
            including timestamps for data stored in the catalog. Value can be an
            exact match, or a regex pattern if operator is set to "~"
        """
        if operator not in self.MATCH_OPERATORS:
            raise Exception(u"Unsupported Operator '%s', must be one of %s" % (operator, u','.join(self.MATCH_OPERATORS)))

        return self.query_nodes(u'["%s", "name", "%s"]' % (operator, name,))

    def get_node_facts(self, node):
        """ Returns all facts for the given node
        """
        return self.query_node_facts(node, u'')

    def get_node_fact(self, node, fact, operator=u'='):
        """ Returns the value of a single fact for the given node
        """
        if operator not in self.MATCH_OPERATORS:
            raise Exception(u"Unsupported Operator '%s', must be one of %s" % (operator, u','.join(self.MATCH_OPERATORS)))

        return self.query_node_facts(node, u'["%s", "name", "%s"]' % (operator, fact))

    def get_node_resources(self, node, resource_type=None):
        """ Returns all resources for the given node
            If the optional resource_type is specified, the results will be filtered to that type.
        """
        if resource_type:
            return self.query_node_resources(node, u'["=", "type", "%s"]' % (resource_type,))
        else:
            return self.query_node_resources(node, u'')

    def get_node_resource(self, node, resource_type, title, operator='='):
        """ Returns the value of a single resource for the given node
        """
        if operator not in self.MATCH_OPERATORS:
            raise Exception(u"Unsupported Operator '%s', must be one of %s" % (operator, u','.join(self.MATCH_OPERATORS)))

        return self.query_node_resources(node, u'["and", ["=", "type", "%s"], ["%s", "title", "%s"]]' % (resource_type, operator, title,))

    def get_node_catalog(self, node):
        """ Returns the most recently compiled catalog for the given node
        """
        return self.query(u'/v3/catalogs/%s' % (node,), None)

    def get_fact_with_value(self, fact, value, operator=u"="):
        """ Returns the fact with a matching value for all nodes. Uses an equality comparison
            by default, but with operator set appropriately the value can also be a
            pattern or inequality.
        """
        if operator not in self.BINARY_OPERATORS:
            raise Exception(u"Unsupported Operator '%s', must be one of %s" % (operator, u','.join(self.BINARY_OPERATORS)))

        return self.query_facts(u'["and", ["=","name","%s"], ["%s","value","%s"]]' % (fact, operator, value))

    def get_facts(self, fact, operator='='):
        """ Returns the values of all facts matching the given name for all nodes
        """
        if operator not in self.MATCH_OPERATORS:
            raise Exception(u"Unsupported Operator '%s', must be one of %s" % (operator, ','.join(self.MATCH_OPERATORS)))

        return self.query_facts(u'["%s","name","%s"]' % (operator,fact,))

    def get_fact(self, fact):
        """ Returns the values of the given fact for all nodes
        """
        return self.get_facts(fact)


    def get_resource(self, resource_type, title, operator=u'='):
        """ Returns the resource of the given type and title for all nodes.
            Does an equality test on title by default, but if operator is set to '~',
            the title may be a pattern for a regex match.
        """
        if operator not in self.MATCH_OPERATORS:
            raise Exception(u"Unsupported Operator '%s', must be one of %s" % (operator, u','.join(self.MATCH_OPERATORS)))

        return self.query_resources(u'["and", ["=", "type", "%s"], ["%s", "title", "%s"]]' % (resource_type, operator, title))

