import os
import heapq
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruits_by_client = {}

    # Misma logica que en sum.
    def _process_data(self, client_id, fruit, amount):
        fruits = self.fruits_by_client.setdefault(client_id, {})
        fruits[fruit] = fruits.get(fruit, fruit_item.FruitItem(fruit, 0)) + (
            fruit_item.FruitItem(fruit, amount)
        )

    # Misma logica de EOF, pero calculando tmb un top parcial a partir de los datos registrados.
    # Envia por la cola los resultados con el formato [client_id, [(fruta, cantidad), ...]].
    def _process_eof(self, client_id):
        logging.info(f"EOF de {client_id}: calculando top parcial")
        fruits = self.fruits_by_client.pop(client_id, {})
        partial_top = heapq.nlargest(TOP_SIZE, fruits.values())
        self.output_queue.send(
            message_protocol.internal.serialize(
                [client_id, [(item.fruit, item.amount) for item in partial_top]]
            )
        )

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()