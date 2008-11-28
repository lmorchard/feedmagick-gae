"""
"""
import datetime, time, md5, logging

from google.appengine.ext import db
from google.appengine.api import memcache
from google.appengine.ext import webapp

class MessageQueueRequestHandler(webapp.RequestHandler):

    def __init__(self):
        self.log = logging.getLogger()
        self.queue = MessageQueue()

    def get(self):
        """ """
        pass

class MessageQueue:
    """
    Implementation of a message queue for async processing.
    """
    MUTEX_KEY = 'decafbad/messagequeue/mutex'

    def __init__(self):
        """
        Initialize the message queue
        """
        self.log = logging.getLogger()
        self.listeners = []

    def flush_all(self):
        """
        Clear out the Message queue
        """
        # TODO: Run this a few times to clear out > 1000 Messages?
        db.delete(QueuedMessage.all())
        db.delete(QueuedMessageDependency.all())

    def put(self, subject='', body='', scheduled_for=None, priority=0, 
            dependencies=None, allow_duplicate=True):
        """
        Put a Message into the queue
        """
        def do_put():

            message = QueuedMessage(
                subject=subject, 
                body=body, 
                priority=priority, 
                scheduled_for=scheduled_for
            )

            if not allow_duplicate:
                # Skip saving the message if one already exists with the new
                # one's signature
                new_sig = message._make_signature()
                existing = QueuedMessage.all()\
                    .filter("signature =", new_sig).fetch(1)
                if existing: return None

            # Save the message and create any necessary dependencies.
            message.put()
            if dependencies:
                for dep in dependencies:
                    message.add_dependency(dep)
            
            return message

        return self.run_with_lock(do_put)

    def reserve(self):
        """
        Reserve a message from the queue for exclusive processing
        """
        def do_reservation():
            # Try grabbing all unreserved and unfinished Messages, in order of
            # creation and priority.
            messages = QueuedMessage.gql("""
                WHERE reserved_at=:1 AND finished_at=:1 
                ORDER BY created_at, priority DESC
            """, None).fetch(1000)
            
            now = datetime.datetime.now()
            for message in messages:

                # Skip scheduled messages not yet ready to run
                if message.scheduled_for and message.scheduled_for > now:
                    continue

                # Skip messages that are depending on at least 1 other message.
                dependencies = QueuedMessageDependency.all()\
                    .filter("dependent_message =", message).fetch(1)
                if dependencies: continue

                # Set the reserved date on the message and stop looking.
                message.reserved_at = now
                message.put()
                return message

        return self.run_with_lock(do_reservation)

    def add_listener(self, subject_pattern, listener):
        """
        Register a message listener interested in a subject
        """
        self.listeners.append( (subject_pattern, listener) )

    def add_listeners(self, listeners):
        """
        Add a list of listeners, expected as a list of (subject, listener)
        tuple pairs.
        """
        self.listeners.extend(listeners)

    def process(self):
        """
        Reserve and process a message by handing it to all interested 
        listeners.
        """
        # Reserve the latest message, return false if none available.
        message = self.reserve()
        if not message: return None

        # Run through all registered listeners looking for any interested in
        # the subject of this message.
        for subject, listener in self.listeners:
            if message.subject == subject:
                try:
                    listener(message)
                except:
                    # TODO: Do something with an exception here
                    pass

        # Mark the message as finished and return it.
        message.finish()
        return message

    def run_with_lock(self, func):
        """
        For creation of Message groups with dependencies, this method allows the
        whole process to run within an exclusive lock.
        """
        self.lock()
        rv = None
        try:
            rv = func()
        finally:    
            self.unlock()
        return rv

    def lock(self):
        """
        Attempt to set a mutex in memcache to lock the queue for serial access.
        """
        while memcache.add(key=self.MUTEX_KEY, value='1', time=1) == False:
            time.sleep(0.01)

    def unlock(self):
        """
        Unlock the queue.
        """
        memcache.delete(key=self.MUTEX_KEY)

class QueuedMessage(db.Model):
    """
    Representation of a queued message
    """
    created_at    = db.DateTimeProperty(auto_now_add=True)
    updated_at    = db.DateTimeProperty(auto_now=True)
    scheduled_for = db.DateTimeProperty(default=None)
    reserved_at   = db.DateTimeProperty(default=None)
    finished_at   = db.DateTimeProperty(default=None)
    priority      = db.IntegerProperty(default=0)
   
    subject       = db.StringProperty(required=True)
    body          = db.BlobProperty(required=True)
    signature     = db.StringProperty()
    
    # TODO: retries?

    def finish(self):
        """
        Mark a message as finished and delete any dependencies.
        """
        # Mark when the Message was finished.
        self.finished_at = datetime.datetime.now()
        self.put()

        # Delete all dependencies on this Message.
        db.delete(QueuedMessageDependency.all().filter("preceding_message =", self))

    def add_dependency(self, preceding_message):
        """
        Create a dependency between this Message and a preceding Message required to be
        finished before this one.
        """
        dependency = QueuedMessageDependency(
            preceding_message=preceding_message, 
            dependent_message=self
        )
        dependency.put()

    def _make_signature(self):
        """
        Build a signature from significant attributes of the message
        """
        return md5.new(','.join(
            '%s=%s' % (key, getattr(self, key))
            for key in [ 'subject', 'body' ]
        )).hexdigest()

    def put(self):
        """
        Save the message, updating signature and anything else necessary.
        """
        self.signature = self._make_signature()
        db.Model.put(self)

class QueuedMessageDependency(db.Model):
    """
    Depencency link between two Messages
    """
    # TODO: Do not allow circular dependencies
    preceding_message = db.ReferenceProperty(QueuedMessage, 
        collection_name='preceding_message')
    dependent_message = db.ReferenceProperty(QueuedMessage, 
        collection_name='dependent_message')

