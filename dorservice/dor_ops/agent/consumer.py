'''
Created on 2017年8月23日

@author: walter628
'''
# This code is based on the example here:
# http://pika.readthedocs.org/en/latest/examples/asynchronous_consumer_example.html
# There are a few significant changes from the example having to do with queue
# type, reconnection upon initial errors connecting, etc.
#
# This queue consumer model assumes that we're doing bi-directional
# verification of all messages received. This means your message_callback
# had better be idempotent.
#
# To generate the amqp URL, see:
# https://pika.readthedocs.org/en/latest/examples/using_urlparameters.html
# Also note that in order to work correctly, you will almost certianly want to
# set the heartbeat to a positive value (say, 10 seconds).

import logging
import pika
import socket
import time

from dor_ops.common.response import DorErrorResponse
from dor_ops.agent.agentconfig import (APP_ID,
                             DOR_EXCHANGE,
                             DEFAULT_EXCHANGE,
                             DIRECT_EXCHANGE_TYPE,
                             DEFAULT_CONSUME_QUEUE)
from bpworker.requests import RequestFactory

from jsonrpc.jsonrpc2 import JSONRPC20Request

LOG = logging.getLogger(__name__)


class BpworkerConsumer(object):

    """This is an AMQP consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """

    def __init__(self, connection_params, exchange=DOR_EXCHANGE,
                 exchange_type=DIRECT_EXCHANGE_TYPE, queue=DEFAULT_CONSUME_QUEUE,
                 routing_key=None, handling_services=None):
        """Create a new instance of the consumer class

        :param str connection_params: pika.URLParameters or
                                      pika.ConnectionParameters
        :param str exchange: The exchange which the consume queue is attached
        :param str queue: The queue to consume
        :routing_key: The routing key to use

        """
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self.connection_params = connection_params
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self._routing_key = routing_key
        self._handling_services = handling_services or []

    @property
    def routing_key(self):
        if not self._routing_key:
            self._routing_key = "%s.%s" % (DEFAULT_CONSUME_QUEUE, socket.gethostname())
        return self._routing_key

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        LOG.debug('Connecting to %s', self.connection_params)
        return pika.SelectConnection(
            self.connection_params,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_open_connection_error,
            stop_ioloop_on_close=False)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOG.debug('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        LOG.debug('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOG.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                        reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def on_open_connection_error(self, unused_connection, error_message=None):
        """This method is called by pika if there's a problem opening a
        connection to the RabbitMQ server. We are stubborn and will keep trying
        to connect after a short delay.

        :type unused_connection: pika.SelectConnection

        """
        LOG.warning('Error opening connection. Retrying.')
        time.sleep(5)
        self.reconnect()

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        LOG.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        if self._connection:
            self._connection.ioloop.stop()

        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOG.debug('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        LOG.warning('Channel %i was closed: (%s) %s',
                    channel, reply_code, reply_text)
        self._connection.close()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOG.debug('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOG.debug('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.exchange_type,
                                       durable=True)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response
            frame
        """
        LOG.debug('Exchange declared')
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOG.debug('Declaring queue %s', queue_name)
        # set default message ttl to 1h (in milliseconds)
        queue_args = {"x-message-ttl": 60 * 60 * 1000}
        self._channel.queue_declare(self.on_queue_declareok,
                                    queue_name,
                                    arguments=queue_args,
                                    durable=True)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOG.debug('Binding %s to %s with %s',
                  self.exchange, self.queue, self.routing_key)
        self._channel.queue_bind(self.on_bindok, self.queue,
                                 self.exchange, self.routing_key)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        LOG.debug('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOG.info('Consumer was cancelled remotely, shutting down: %r',
                 method_frame)
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOG.debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def rpc_response(self, channel, method, header, response):
        """Sends a response back to the RPC caller.

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver: method
        :param pika.Spec.BasicProperties: header
        :param str|unicode response: The response to send

        """
        # No response needed for one-way messages
        if not header.reply_to:
            LOG.info("Not sending response for one-way call: %s", response)
            return

        LOG.info("Sending response to RPC call, reply_to queue: %s" +
                 ", payload: %s", header.reply_to, response)

        channel.basic_publish(
            exchange=DEFAULT_EXCHANGE,
            routing_key=header.reply_to,
            properties=pika.BasicProperties(
                app_id=APP_ID,
                content_type='application/json',
                correlation_id=header.correlation_id,
                delivery_mode=2,  # make message persistent
            ),
            body=response
        )

    def _decode_unknown_request(self, request_text):
        try:
            return JSONRPC20Request(request_text)
        except Exception as e:
            raise Exception("Unable to parse request: %s. Ignoring." % e)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        LOG.debug('Received message # %s from %s: %s',
                  basic_deliver.delivery_tag, properties.app_id, body)
        request = None
        response = None

        try:
            handled = False
            request = RequestFactory.create_request_from_json(body)
            request.header = properties

            for service in self._handling_services:
                LOG.debug("Looking for a handler for '%s'", request.service)
                if request.service == service.request_type:
                    try:
                        response = service.handle_request(request)
                    except Exception as e:
                        raise Exception(e)
                    handled = True

            if not handled:
                raise Exception("Not able to handle request: %s" %
                                request.__class__.__name__)
        except Exception as e:
            if not request:
                request = self._decode_unknown_request(body)

            response = BPWErrorResponse(request._id, -1, str(e))
            LOG.error("Problem with incoming message: %s" % e)

        if response:
            self.rpc_response(unused_channel, basic_deliver, properties,
                              response.json)

        self.acknowledge_message(basic_deliver.delivery_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        LOG.debug('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            LOG.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        LOG.debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.queue)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        LOG.debug('Queue bound')
        self.start_consuming()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        LOG.debug('Closing the channel')
        self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        LOG.debug('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        LOG.info('Starting')
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        LOG.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        LOG.info('Stopped')
