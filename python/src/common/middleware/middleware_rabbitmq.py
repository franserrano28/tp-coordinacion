import threading
import time

import pika
import pika.exceptions

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

CONNECTION_RETRIES = 10
CONNECTION_RETRY_DELAY_SECONDS = 1

_DISCONNECTION_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.StreamLostError,
    pika.exceptions.ConnectionClosed,
)


class _RabbitMQBase:

    def __init__(self, host):
        self._connection = self._connect(host)
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._consuming = False
        self._consumer_thread_id = None

    @staticmethod
    def _connect(host):
        last_error = None
        for _ in range(CONNECTION_RETRIES):
            try:
                return pika.BlockingConnection(pika.ConnectionParameters(host=host))
            except pika.exceptions.AMQPConnectionError as e:
                last_error = e
                time.sleep(CONNECTION_RETRY_DELAY_SECONDS)
        raise MessageMiddlewareDisconnectedError(str(last_error))

    def _consume_queue_names(self):
        raise NotImplementedError

    def _publish(self, message):
        raise NotImplementedError

    def start_consuming(self, on_message_callback):
        def _on_message(channel, method, _properties, body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        try:
            for queue_name in self._consume_queue_names():
                self._channel.basic_consume(
                    queue=queue_name, on_message_callback=_on_message
                )
            self._consuming = True
            self._consumer_thread_id = threading.get_ident()
            self._channel.start_consuming()
        except _DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(str(e))
        finally:
            self._consuming = False

    def stop_consuming(self):
        if not self._consuming:
            return
        try:
            if threading.get_ident() == self._consumer_thread_id:
                self._channel.stop_consuming()
            else:
                self._connection.add_callback_threadsafe(self._channel.stop_consuming)
        except _DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    def send(self, message):
        try:
            self._publish(message)
        except _DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            if self._channel.is_open:
                self._channel.close()
            if self._connection.is_open:
                self._connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(str(e))


class MessageMiddlewareQueueRabbitMQ(_RabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        _RabbitMQBase.__init__(self, host)
        self._queue_name = queue_name
        self._channel.queue_declare(queue=queue_name)

    def _consume_queue_names(self):
        return [self._queue_name]

    def _publish(self, message):
        self._channel.basic_publish(
            exchange="", routing_key=self._queue_name, body=message
        )


class MessageMiddlewareExchangeRabbitMQ(_RabbitMQBase, MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        _RabbitMQBase.__init__(self, host)
        self._exchange_name = exchange_name
        self._routing_keys = list(routing_keys)
        self._channel.exchange_declare(exchange=exchange_name, exchange_type="direct")
        self._queue_names = []
        for key in self._routing_keys:
            queue_name = f"{exchange_name}_{key}"
            self._channel.queue_declare(queue=queue_name)
            self._channel.queue_bind(
                queue=queue_name, exchange=exchange_name, routing_key=key
            )
            self._queue_names.append(queue_name)

    def _consume_queue_names(self):
        return self._queue_names

    def _publish(self, message):
        for key in self._routing_keys:
            self._channel.basic_publish(
                exchange=self._exchange_name, routing_key=key, body=message
            )