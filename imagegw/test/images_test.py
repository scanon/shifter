import unittest
from time import time
import json
from pymongo import MongoClient

"""
Shifter, Copyright (c) 2015, The Regents of the University of California,
through Lawrence Berkeley National Laboratory (subject to receipt of any
required approvals from the U.S. Dept. of Energy).  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
 1. Redistributions of source code must retain the above copyright notice,
    this list of conditions and the following disclaimer.
 2. Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.
 3. Neither the name of the University of California, Lawrence Berkeley
    National Laboratory, U.S. Dept. of Energy nor the names of its
    contributors may be used to endorse or promote products derived from this
    software without specific prior written permission.`

See LICENSE for full text.
"""


class ImagesTestCase(unittest.TestCase):

    def setUp(self):
        from shifter_imagegw.images import Images
        self.configfile = 'test.json'
        with open(self.configfile) as config_file:
            self.config = json.load(config_file)
        mongodb = self.config['MongoDBURI']
        client = MongoClient(mongodb)
        db = self.config['MongoDB']
        self.images = client[db].images
        self.metrics = client[db].metrics
        self.images.drop()
        self.m = Images(mongodb, db, metrics=True)
        self.system = 'systema'
        self.itype = 'docker'
        self.tag = 'test'
        self.id = 'fakeid'
        self.tag2 = 'test2'
        self.tag3 = 'scanon/shanetest:latest'
        self.public = 'index.docker.io/busybox:latest'
        self.private = 'index.docker.io/scanon/shaneprivate:latest'
        self.format = 'squashfs'
        self.auth = 'good:user:user::100:100'
        self.authadmin = 'good:root:root::0:0'
        self.badauth = 'bad:user:user::100:100'
        self.logfile = '/tmp/worker.log'
        self.pid = 0
        self.query = {'system': self.system, 'itype': self.itype,
                      'tag': self.tag}
        # Cleanup Mongo
        self.images.remove({})

    def tearDown(self):
        """
        tear down should stop the worker
        """
        pass

    def good_pullrecord(self):
        return {'system': self.system,
                'itype': self.itype,
                'id': self.id,
                'pulltag': self.tag,
                'status': 'READY',
                'userACL': [],
                'groupACL': [],
                'ENV': [],
                'ENTRY': '',
                'last_pull': time()
                }

    def good_record(self):
        return {
            'system': self.system,
            'itype': self.itype,
            'id': self.id,
            'tag': [self.tag],
            'format': self.format,
            'status': 'READY',
            'userACL': [],
            'groupACL': [],
            'last_pull': time(),
            'ENV': [],
            'ENTRY': ''
        }

    def set_last_pull(self, id, t):
        self.m.images.update({'_id': id}, {'$set': {'last_pull': t}})

#
#  Tests
#

# Create an image and tag with a new tag.
# Make sure both tags show up.
# Remove the tag and make sure the original tags
# doesn't also get removed.
    def test_0add_remove_tag(self):
        record = self.good_record()
        # Create a fake record in mongo
        id = self.images.insert(record)

        assert id is not None
        before = self.images.find_one({'_id': id})
        assert before is not None
        # Add a tag a make sure it worked
        status = self.m.add_tag(id, self.system, 'testtag')
        assert status is True
        after = self.images.find_one({'_id': id})
        assert after is not None
        assert after['tag'].count('testtag') == 1
        assert after['tag'].count(self.tag) == 1
        # Remove a tag and make sure it worked
        status = self.m.remove_tag(self.system, 'testtag')
        assert status is True
        after = self.images.find_one({'_id': id})
        assert after is not None
        assert after['tag'].count('testtag') == 0

    # Similar to above but just test the adding part
    def test_0add_remove_tagitem(self):
        record = self.good_record()
        record['tag'] = self.tag
        # Create a fake record in mongo
        id = self.images.insert(record)

        status = self.m.add_tag(id, self.system, 'testtag')
        assert status is True
        rec = self.images.find_one({'_id': id})
        assert rec is not None
        assert rec['tag'].count(self.tag) == 1
        assert rec['tag'].count('testtag') == 1

    # Same as above but use the lookup instead of a directory
    # direct mongo lookup
    def test_0add_remove_withtag(self):
        record = self.good_record()
        # Create a fake record in mongo
        id = self.images.insert(record)

        status = self.m.add_tag(id, self.system, 'testtag')
        assert status is True
        rec = self.m.get_image_by_id(id)
        assert rec is not None
        assert rec['tag'].count(self.tag) == 1
        assert rec['tag'].count('testtag') == 1

    # Test if tag isn't a list
    def test_0add_remove_two(self):
        record = self.good_record()
        # Create a fake record in mongo
        id1 = self.images.insert(record.copy())
        record['id'] = 'fakeid2'
        record['tag'] = []
        id2 = self.images.insert(record.copy())

        status = self.m.add_tag(id2, self.system, self.tag)
        assert status is True
        rec1 = self.images.find_one({'_id': id1})
        rec2 = self.images.find_one({'_id': id2})
        assert rec1['tag'].count(self.tag) == 0
        assert rec2['tag'].count(self.tag) == 1

    # Similar to above but just test the adding part
    def test_0add_same_image_two_system(self):
        record = self.good_record()
        record['tag'] = self.tag
        # Create a fake record in mongo
        id1 = self.images.insert(record.copy())
        # add testtag for systema
        status = self.m.add_tag(id1, self.system, 'testtag')
        assert status is True
        record['system'] = 'systemb'
        id2 = self.images.insert(record.copy())
        status = self.m.add_tag(id2, 'systemb', 'testtag')
        assert status is True
        # Now make sure testtag for first system is still
        # present
        rec = self.images.find_one({'_id': id1})
        assert rec is not None
        assert rec['tag'].count('testtag') == 1

    def test_0resetexp(self):
        record = {'system': self.system,
                  'itype': self.itype,
                  'id': self.id,
                  'pulltag': self.tag,
                  'status': 'READY',
                  'userACL': [],
                  'groupACL': [],
                  'ENV': [],
                  'ENTRY': '',
                  'last_pull': 0
                  }
        id = self.images.insert(record.copy())
        assert id is not None
        expire = time() + 1000
        self.m.reset_image_expire(id, expire)
        rec = self.images.find_one({'_id': id})
        assert rec['expiration'] == expire

    def test_0update_image_states(self):
        # Test a repull
        record = self.good_record()
        # Create a fake record in mongo
        id = self.images.insert(record)
        assert id is not None
        status = 'TESTME'
        self.m.update_image_state(id, status)
        rec = self.images.find_one({'_id': id})
        self.assertIsNotNone(rec)
        self.assertEquals(rec['status'], status)

    def test_0update_states(self):
        # Test a repull
        record = self.good_record()
        record['last_pull'] = 0
        record['status'] = 'FAILURE'
        # Create a fake record in mongo
        id = self.images.insert(record)
        assert id is not None
        self.m.update_states()
        rec = self.images.find_one({'_id': id})
        assert rec is None

    def test_lookup(self):
        record = self.good_record()
        record['expiration'] = time()+100
        # Create a fake record in mongo
        self.images.insert(record)
        myid = record['id']
        system = record['system']
        l = self.m.get_image_by_imageid(myid, system)
        assert 'status' in l
        assert '_id' in l
        assert self.m.get_state(l['_id']) == 'READY'
        i = self.query.copy()
        r = self.images.find_one({'_id': l['_id']})
        assert 'expiration' in r
        assert r['expiration'] > time()
        i['tag'] = 'bogus'
        l = self.m.get_image_by_id(i)
        assert l is None

    def test_insert_image(self):
        pass

    def test_list(self):
        record = self.good_record()
        # Create a fake record in mongo
        id1 = self.images.insert(record.copy())
        # rec2 is a failed pull, it shouldn't be listed
        rec2 = record.copy()
        rec2['status'] = 'FAILURE'
        li = self.m.get_images(self.system)
        assert len(li) == 1
        l = li[0]
        assert '_id' in l
        assert self.m.get_state(l['_id']) == 'READY'
        assert l['_id'] == id1

    def test_metrics(self):
        rec = {
            "uid": 100,
            "user": "usera",
            "tag": self.tag,
            "system": self.system,
            "id": self.id,
            "type": self.itype
        }
        # Remove everything
        self.metrics.remove({})
        for _ in xrange(100):
            rec['time'] = time()
            self.metrics.insert(rec.copy())
        recs = self.m.get_metrics(self.system, 10)  # ,delay=False)
        self.assertIsNotNone(recs)
        self.assertEquals(len(recs), 10)
        # Try pulling more records than we have
        recs = self.m.get_metrics(self.system, 101)  # ,delay=False)
        self.assertIsNotNone(recs)
        self.assertEquals(len(recs), 100)


if __name__ == '__main__':
    unittest.main()
