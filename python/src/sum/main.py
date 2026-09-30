import os
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_fruit_by_client = {}

    # Obtiene el diccionario de frutas de un cliente, si no existe lo crea.
    # Guarda el acumulado de una fruta para un cliente y le suma una cantidad nueva.
    def _process_data(self, client_id, fruit, amount):
        amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    # El cliente envia EOF y sum le envia los totales a aggregation seguidos por el ID del cliente.
    def _process_eof(self, client_id):
        logging.info(f"EOF de {client_id}: enviando totales")
        amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})
        for fruit_item in amount_by_fruit.values():
            for exchange in self.data_output_exchanges:
                exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, fruit_item.fruit, fruit_item.amount]
                    )
                )

        for exchange in self.data_output_exchanges:
            exchange.send(message_protocol.internal.serialize([client_id]))

    # Procesa los mensajes recibidos por la cola de entrada y define si es de tipo dato o eof.
    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()