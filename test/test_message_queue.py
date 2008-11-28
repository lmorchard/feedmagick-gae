"""
Message queue tests
"""
import unittest, logging, datetime, time
import messagequeue

from google.appengine.ext import db
from google.appengine.api import memcache

class TestMessageQueue(unittest.TestCase):

    def setUp(self):
        self.log = logging.getLogger()
        self.log.setLevel(logging.DEBUG)

        self.queue = messagequeue.MessageQueue()
        self.queue.flush_all()

        memcache.flush_all()

    def tearDown(self):
        memcache.flush_all()

    def test_memcache_mutex(self):
        """Try out a memcache-based mutex"""
        self.assert_(memcache.add(key='mutex', value='foo') == True)
        self.assert_(memcache.add(key='mutex', value='foo') == False)

    def test_put_reserve(self):
        """
        Queue some messages and make sure they get reserved in the same order and
        with the same properties.  Also, make sure reserve returns False when
        all the messages have been reserved.
        """
        subject  = '/tests/message'
        bodies = []
        
        # Create a bunch of messages
        for i in range(10):
            body = 'sample body %s' % i
            bodies.append(body)
            self.queue.put(subject=subject, body=body)

        # Reserve and verify contents of known messages
        for body in bodies:
            message = self.queue.reserve()
            self.assert_(message is not False)
            self.assertEqual(subject, message.subject)
            self.assertEqual(body, message.body)
        
        # There should be no more messages to reserve
        self.assert_(self.queue.reserve() is None)

    def test_scheduled_for(self):
        """
        Make sure a scheduled message doesn't get reserved until after its
        scheduled time.
        """
        delay = 1
        now   = datetime.datetime.now()
        later = now + datetime.timedelta(seconds=delay)

        self.queue.put(
            subject='/tests/delayed_message', 
            body='sample body', 
            scheduled_for=later
        )

        self.assert_(self.queue.reserve() is None)
        time.sleep(delay + 1)
        self.assert_(self.queue.reserve() is not None)

    def test_finish(self):
        """
        Make sure finished messages get finished datestamps and cannot be
        reserved
        """
        subject = '/tests/message/finish'
        messages  = []
        
        # Create a bunch of messages
        for i in range(10):
            body = 'sample body %s' % i
            message = self.queue.put(subject=subject, body=body)
            messages.append(message)

        # Assert that none of the messages are finished
        finished_messages = messagequeue.QueuedMessage.gql("""
            WHERE finished_at != :1 
        """, None).fetch(1000)
        self.assertEqual(len(finished_messages), 0)

        # Assert that at least one message can be reserved
        self.assert_(self.queue.reserve() is not None)

        # Finish all known messages
        for message in messages: message.finish()

        # Assert that all the messages have finished datestamps
        finished_messages = messagequeue.QueuedMessage.gql("""
            WHERE finished_at != :1 
        """, None).fetch(1000)
        self.assertEqual(len(finished_messages), len(messages))

        # Assert that no more messages can be reserved
        self.assert_(self.queue.reserve() is None)

    def test_dependency(self):
        """
        Create a small set of dependent messages and ensure that reservation of
        messages whose dependent messages are not yet finished cannot be reserved.
        """

        def make_message_group():
            subject = '/tests/foo'

            message1 = self.queue.put(subject=subject, body='message1')
            message2 = self.queue.put(subject=subject, body='message2')
            message3 = self.queue.put(subject=subject, body='message3')
            message5 = self.queue.put(subject=subject, body='message5')

            message2.add_dependency(message1)
            message3.add_dependency(message2)

            message4 = self.queue.put(subject=subject, body='message4',
                dependencies=[message3, message5])
            
        # Queue up a dependent group of messages in a lock.
        self.queue.run_with_lock(make_message_group)

        # Message 1 comes first.
        t_message1 = self.queue.reserve()
        self.assertEqual(t_message1.body, 'message1')

        # Message 5 comes next.
        t_message5 = self.queue.reserve()
        self.assertEqual(t_message5.body, 'message5')

        # The rest of the messages are waiting on dependencies.
        self.assert_(self.queue.reserve() is None)

        # Finishing message 1 should make message 2 available.
        t_message1.finish()
        t_message2 = self.queue.reserve()
        self.assertEqual(t_message2.body, 'message2')

        # The rest of the messages are waiting on dependencies.
        self.assert_(self.queue.reserve() is None)

        # Finishing message 2 should make message 3 available.
        t_message2.finish()
        t_message3 = self.queue.reserve()
        self.assertEqual(t_message3.body, 'message3')
        self.assert_(self.queue.reserve() is None)

        # Only when both messages 3 and 5 are finished, should message 4 become
        # available.
        t_message3.finish()
        self.assert_(self.queue.reserve() is None)
        t_message5.finish()
        t_message4 = self.queue.reserve()
        self.assertEqual(t_message4.body, 'message4')
        self.assert_(self.queue.reserve() is None)

    def test_duplicate_signature(self):
        """
        Try out optional duplicate message restriction.
        """
        # Queue a couple of intentionally duplicate messages
        message1 = self.queue.put(subject='test subject', body='test body')
        message2 = self.queue.put(subject='test subject', body='test body')
        self.assertEqual(message1.signature, message2.signature)

        # Try queueing a duplicate message with allow_duplicate=False 
        # and expect failure
        message3 = self.queue.put(subject='test subject', body='test body', 
            allow_duplicate=False)
        self.assert_(message3 is None)

        # Just for good measure, make sure only 2 messages can be reserved.
        self.assert_(self.queue.reserve() is not None)
        self.assert_(self.queue.reserve() is not None)
        self.assert_(self.queue.reserve() is None)

    def test_listen_process(self):
        """
        Try out pairing listeners with a queue of messages and run through 
        a message processing loop.
        """
        # Build a set of listeners with a results collector
        class listeners:
            results = []
            def listener1(self, message):
                self.results.append('l1-%s' % message.body)
            def listener2(self, message):
                self.results.append('l2-%s' % message.body)
            def listener3(self, message):
                self.results.append('l3-%s' % message.body)
        inst = listeners()

        # Add listeners to the message queue.
        self.queue.add_listener('/subject1', inst.listener1)
        self.queue.add_listeners([
            ( '/subject2', inst.listener2 ),
            ( '/subject3', inst.listener3 ),
            ( '/subject2and3', inst.listener2 ),
            ( '/subject2and3', inst.listener3 )
        ])

        # Queue a bunch of messages to be listened to by the above.
        messages = [
            self.queue.put(subject='/subject1', body='b1'),
            self.queue.put(subject='/subject1', body='b2'),
            self.queue.put(subject='/subject2', body='b3'),
            self.queue.put(subject='/uninteresting', body='whocares'),
            self.queue.put(subject='/subject3', body='b4'),
            self.queue.put(subject='/uninteresting', body='whocares'),
            self.queue.put(subject='/subject2and3', body='b5'),
            self.queue.put(subject='/uninteresting', body='whocares'),
            self.queue.put(subject='/subject2and3', body='b6')
        ]

        # Process until unable to process, and make sure the iterations match
        # count of queued messages
        cnt = 0
        while self.queue.process(): cnt = cnt + 1
        self.assertEqual(cnt, len(messages))

        # Verify the results of processing all messages in the queue
        self.assertEqual(
            ','.join(inst.results),
            'l1-b1,l1-b2,l2-b3,l3-b4,l2-b5,l3-b5,l2-b6,l3-b6'
        )

        # Finally, there should be no more messages left to reserve
        self.assert_(self.queue.reserve() is None)

    def test_everything(self):
        """ """
        pass

