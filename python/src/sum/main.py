import os
import logging
import threading
import zlib

from common import middleware, message_protocol, fruit_item, shutdown

internal = message_protocol.internal

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


# Envia mensajes a los aggregators. Las conexiones de pika no son thread-safe,
# por eso cada hilo de sum crea su propia instancia.
class AggregationSender:

    def __init__(self):
        self.exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

    def _broadcast(self, message):
        serialized = internal.serialize(message)
        for exchange in self.exchanges:
            exchange.send(serialized)

    # Envia a aggregation el total acumulado de una fruta para un cliente.
    # Cada fruta tiene un aggregator dueño. Usamos zlib para hashear y crc32
    # (no criptografico ni aleatorio)
    def send_data(self, client_id, fruit, amount):
        owner = zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT
        self.exchanges[owner].send(
            internal.serialize([internal.DATA, client_id, fruit, amount])
        )

    # La cantidad de registros originales que cubrio un hilo.
    def send_count(self, client_id, records):
        self._broadcast([internal.COUNT, client_id, records])

    # El total de registros que envió el cliente.
    def send_total(self, client_id, total_records):
        self._broadcast([internal.TOTAL, client_id, total_records])


# Estado parcial del cliente en esta replica.
class ClientState:

    def __init__(self):
        self.amount_by_fruit = {}
        self.records = 0

    def add(self, fruit, amount):
        self.amount_by_fruit[fruit] = self.amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))
        self.records += 1

class SumFilter:

    # Ahora tenemos dos threads, el main y el que maneja el exchange de control.
    def __init__(self):
        self.lock = threading.Lock()
        self.state_by_client = {}
        self.closed_clients = set()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.aggregation_sender = AggregationSender()
        self.control_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, self._control_keys(range(SUM_AMOUNT))
        )
        # Conexiones que usa solo el hilo de control.
        # Se crean aca para que stop() y close() puedan alcanzarlas.
        self.control_sender = AggregationSender()
        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, self._control_keys([ID])
        )
        self.control_thread = threading.Thread(target=self._run_control)

    # Construye las claves de control
    @staticmethod
    def _control_keys(ids):
        return [f"{SUM_PREFIX}_{i}" for i in ids]

    # Tomo lo acumulado, lo mando, mando la cantidad y borro el estado.
    def _flush(self, client_id, sender):
        state = self.state_by_client.pop(client_id, None)
        if state is None:
            return
        for fruit_item in state.amount_by_fruit.values():
            sender.send_data(
                client_id, fruit_item.fruit, fruit_item.amount
            )
        sender.send_count(client_id, state.records)

    # Acumula el registro. Si el cliente ya fue cerrado, lo reenvia enseguida (nunca le van a mandar FLUSH).
    def _process_data(self, client_id, fruit, amount):
        with self.lock:
            self.state_by_client.setdefault(client_id, ClientState()).add(
                fruit, amount
            )
            if client_id in self.closed_clients:
                self._flush(client_id, self.aggregation_sender)

    # Informa el total esperado al aggregation y avisa a todos los sum.
    def _process_eof(self, client_id, total_records):
        logging.info(f"EOF from {client_id} ({total_records} records)")
        self.aggregation_sender.send_total(client_id, total_records)
        self.control_publisher.send(internal.serialize([internal.FLUSH, client_id]))

    # Procesa los mensajes de la cola de entrada y define si son dato o eof.
    def process_data_messsage(self, message, ack, nack):
        kind, *fields = internal.deserialize(message)
        if kind == internal.DATA:
            self._process_data(*fields)
        elif kind == internal.EOF:
            self._process_eof(*fields)
        ack()

    # Cuando recibe un FLUSH marca al client como cerrado y envia lo acumulado.
    def _run_control(self):
        def process_control_message(message, ack, nack):
            kind, client_id = internal.deserialize(message)
            if kind == internal.FLUSH:
                with self.lock:
                    self.closed_clients.add(client_id)
                    self._flush(client_id, self.control_sender)
            ack()

        try:
            self.control_input.start_consuming(process_control_message)
        except Exception as e:
            logging.error(f"Control thread failed: {e}")

    def start(self):
        self.control_thread.start()
        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            # Si el hilo principal termina (señal o error), frena y espera al
            # de control para no dejarlo consumiendo con conexiones cerradas.
            shutdown.stop_consuming_all(self.control_input)
            self.control_thread.join()

    def stop(self):
        shutdown.stop_consuming_all(self.input_queue, self.control_input)

    def close(self):
        shutdown.close_all(
            self.input_queue,
            self.control_publisher,
            self.control_input,
            *self.aggregation_sender.exchanges,
            *self.control_sender.exchanges,
        )


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    shutdown.on_signal(sum_filter.stop)
    try:
        sum_filter.start()
    finally:
        sum_filter.close()
    return 0


if __name__ == "__main__":
    main()