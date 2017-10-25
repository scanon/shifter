from pymongo import MongoClient
import pymongo.errors
from time import sleep, time

# This module should abstracts all of the database interactions.
#


# decorator function to re-attempt any mongo operation that may have failed
# owing to AutoReconnect (e.g., mongod coming back, etc).  This may increase
# the opportunity for race conditions, and should be more closely considered
# for the insert/update functions
def mongo_reconnect_reattempt(call):
    """Automatically re-attempt potentially failed mongo operations"""
    def _mongo_reconnect_safe(self, *args, **kwargs):
        for _ in xrange(2):
            try:
                return call(self, *args, **kwargs)
            except pymongo.errors.AutoReconnect:
                # self.logger.warn("Error: mongo reconnect attmempt")
                sleep(2)
        # self.logger.warn("Error: Failed to deal with mongo auto-reconnect!")
        raise OSError('Reconnect to mongo failed')
    return _mongo_reconnect_safe


class Images(object):
    def __init__(self, mongo_uri, mongo_db, metrics=False,
                 update_timeout=300):
        client = MongoClient(mongo_uri)
        self.images = client[mongo_db].images
        self.pullupdatetimeout = update_timeout
        if metrics:
            self.metrics = client[mongo_db].metrics
        else:
            self.metrics = None

    def get_image_by_id(self, ident):
        return self._images_find_one({'_id': ident})

    def get_image_by_imageid(self, imageid, system, status=None):
        q = {'id': imageid, 'system': system}
        if status is not None:
            q['status'] = 'READY'
        return self._images_find_one(q)

    def get_image_by_tag(self, image, status='READY'):
        query = {
            'status': status,
            'system': image['system'],
            'itype': image['itype'],
            'tag': {'$in': [image['tag']]}
        }
        rec = self._images_find_one(query)
        return rec

    def get_images(self, system, status='READY'):
        q = dict()
        if system is not None:
            q['system'] = system
        q['status'] = status
        # Special status
        if status == "NOT_READY":
            q['status'] = {'$ne': 'READY'}
        results = []
        for image in self._images_find(q):
            results.append(image)

        return results

    def get_images_by_pulltag(self, image):
        q = {
            'system': image['system'],
            'itype': image['itype'],
            'pulltag': image['pulltag']
        }
        resp = []
        for rec in self._images_find(q):
            resp.append(rec)
        return resp

    def insert_image(self, image):
        # Clean out any existing records
        q = {
            'system': image['system'],
            'itype': image['itype'],
            'pulltag': image['pulltag']
        }
        for rec in self._images_find(q):
            if rec['status'] == 'READY':
                continue
            else:
                self.images.remove_image_by_id(rec['id'])
        return self._images_insert(image)

    def remove_image_by_id(self, ident):
        self._images_remove({'_id': ident})

    def update_image_state(self, ident, state, info=None):
        """
        Helper function to set the mongo state for an image with _id==ident
        to state=state.
        """
        if state == 'SUCCESS':
            state = 'READY'
        set_list = {'status': state, 'status_message': ''}
        if info is not None and isinstance(info, dict):
            if 'heartbeat' in info:
                set_list['last_heartbeat'] = info['heartbeat']
            if 'message' in info:
                set_list['status_message'] = info['message']
        return self._images_update({'_id': ident}, {'$set': set_list})

    def get_state(self, ident):
        """
        Lookup the state of the image with _id==ident in Mongo.
        Returns the state.
        """
        rec = self._images_find_one({'_id': ident}, {'status': 1})
        if rec is None:
            return None
        elif 'status' not in rec:
            return None
        return rec['status']

    def reset_image_expire(self, ident, expire):
        """Reset the expire time.  (Not fully implemented)."""
        self._images_update({'_id': ident}, {'$set': {'expiration': expire}})

    def add_tag(self, ident, system, tag):
        """
        Helper function to add a tag to an image.
        ident is the mongo id (not image id)
        """
        # Remove the tag first
        self.remove_tag(system, tag)
        # see if tag isn't a list
        rec = self._images_find_one({'_id': ident})
        if rec is not None and 'tag' in rec and \
                not isinstance(rec['tag'], (list)):
            # memo = 'Fixing tag for non-list %s %s' % (ident, str(rec['tag']))
            curtag = rec['tag']
            self._images_update({'_id': ident}, {'$set': {'tag': [curtag]}})
        self._images_update({'_id': ident}, {'$addToSet': {'tag': tag}})
        return True

    def remove_tag(self, system, tag):
        """
        Helper function to remove a tag to an image.
        """
        self._images_update({'system': system, 'tag': {'$in': [tag]}},
                            {'$pull': {'tag': tag}}, multi=True)
        return True

    def update_image_last_pull(self, ident, time=time()):
        self.update_mongo(ident, {'last_pull': time})

    def update_states(self):
        """
        Cleanup failed transcations after a period
        """
        for rec in self._images_find({'status': 'FAILURE'}):
            nextpull = self.pullupdatetimeout + rec['last_pull']
            # It it has been a while then let's clean up
            if time() > nextpull:
                self._images_remove({'_id': rec['_id']})

    def update_mongo(self, ident, resp):
        """
        Helper function to set the mongo values for an image with _id==ident.
        """
        setline = dict()
        # This maps from the key name in the response to the
        # key name used in mongo
        mappings = {
            'id': 'id',
            'entrypoint': 'ENTRY',
            'env': 'ENV',
            'workdir': 'WORKDIR',
            'last_pull': 'last_pull',
            'userACL': 'userACL',
            'groupACL': 'groupACL',
            'private': 'private',
            'state': 'status'
        }
        if 'private' in resp and resp['private'] is False:
            resp['userACL'] = []
            resp['groupACL'] = []

        for key in mappings.keys():
            if key in resp:
                setline[mappings[key]] = resp[key]
        self._images_update({'_id': ident}, {'$set': setline})

    def get_metrics(self, system, limit):
        if self.metrics is None:
            return []
        count = self.metrics.count()
        skip = count - limit
        if skip < 0:
            skip = 0
        recs = []
        for r in self.metrics.find().skip(skip):
            r.pop('_id', None)
            recs.append(r)
        return recs

    @mongo_reconnect_reattempt
    def _images_remove(self, *args, **kwargs):
        """ Decorated function to remove images from mongo """
        return self.images.remove(*args, **kwargs)

    @mongo_reconnect_reattempt
    def _images_update(self, *args, **kwargs):
        """ Decorated function to updates images in mongo """
        return self.images.update(*args, **kwargs)

    @mongo_reconnect_reattempt
    def _images_find(self, *args, **kwargs):
        """ Decorated function to find images in mongo """
        return self.images.find(*args, **kwargs)

    @mongo_reconnect_reattempt
    def _images_find_one(self, *args, **kwargs):
        """ Decorated function to find one image in mongo """
        return self.images.find_one(*args, **kwargs)

    @mongo_reconnect_reattempt
    def _images_insert(self, *args, **kwargs):
        """ Decorated function to insert an image in mongo """
        return self.images.insert(*args, **kwargs)

    @mongo_reconnect_reattempt
    def _metrics_insert(self, *args, **kwargs):
        """ Decorated function to insert an image in mongo """
        if self.metrics is not None:
            return self.metrics.insert(*args, **kwargs)
