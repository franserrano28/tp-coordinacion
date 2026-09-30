import os
import heapq
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.pending_by_client = {}

    # Recibe el top parcial de un cliente y lo agrega al diccionario de pendientes.
    # pending = [cantidad_de_tops_recibidos, [FruitItem, ...]]
    def _process_data(self, client_id, partial_top):
        pending = self.pending_by_client.setdefault(client_id, [0, []])

        pending[0] += 1

        pending[1].extend(
            fruit_item.FruitItem(fruit, amount)
            for fruit, amount in partial_top
        )

    # Calcula el top final y lo envía a la cola de salida.
    def _process_eof(self, client_id):
        pending = self.pending_by_client.pop(client_id)

        final_top = heapq.nlargest(TOP_SIZE, pending[1])

        self.output_queue.send(
            message_protocol.internal.serialize(
                [client_id, [(item.fruit, item.amount) for item in final_top]]
            )
        )

    def process_messsage(self, message, ack, nack):
        client_id, partial_top = message_protocol.internal.deserialize(message)

        self._process_data(client_id, partial_top)

        pending = self.pending_by_client[client_id]

        if pending[0] == AGGREGATION_AMOUNT:
            logging.info(f"Top final de {client_id}")
            self._process_eof(client_id)

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()