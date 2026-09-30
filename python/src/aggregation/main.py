from pika import callback
import os
import heapq
import logging

from common import middleware, message_protocol, fruit_item

internal = message_protocol.internal

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


# Estado de un cliente en este aggregator.
class ClientState:

    def __init__(self):
        self.fruits = {}
        self.records_covered = 0
        self.expected_records = None

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.state_by_client = {}

    def _state(self, client_id):
        return self.state_by_client.setdefault(client_id, ClientState())

    def _process_data(self, client_id, fruit, amount):
        fruits = self._state(client_id).fruits
        fruits[fruit] = fruits.get(fruit, fruit_item.FruitItem(fruit, 0)) + (
            fruit_item.FruitItem(fruit, amount)
        )

    # Cuantos registros cubrio un sum.
    def _process_count(self, client_id, records):
        self._state(client_id).records_covered += records
        self._send_top_if_complete(client_id)

    # Total de registros
    def _process_total(self, client_id, total_records):
        self._state(client_id).expected_records = total_records
        self._send_top_if_complete(client_id)

    # Si ya se cubrieron todos los registros del cliente, calcula el top parcial.
    def _send_top_if_complete(self, client_id):
        state = self.state_by_client[client_id]
        if state.expected_records != state.records_covered:
            return

        logging.info(f"Cliente {client_id} completo: calculando top parcial")
        del self.state_by_client[client_id]
        partial_top = heapq.nlargest(TOP_SIZE, state.fruits.values())
        self.output_queue.send(
            internal.serialize(
                [client_id, [(item.fruit, item.amount) for item in partial_top]]
            )
        )

    def process_messsage(self, message, ack, nack):
        kind, *fields = internal.deserialize(message)

        match kind:
            case internal.DATA:
                self._process_data(*fields)
            case internal.COUNT:
                self._process_count(*fields)
            case internal.TOTAL:
                self._process_total(*fields)

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