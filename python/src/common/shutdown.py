import logging
import signal
import threading

def on_signal(callback):
    def run_callback():
        try:
            callback()
        except Exception as e:
            logging.error(f"While stopping: {e}")

    def handler(signum, _frame):
        logging.info(f"Signal {signal.Signals(signum).name} received, shutting down")
        threading.Thread(target=run_callback).start()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def stop_consuming_all(*middlewares):
    for middleware in middlewares:
        try:
            middleware.stop_consuming()
        except Exception as e:
            logging.error(f"While stopping consumption: {e}")


def close_all(*middlewares):
    for middleware in middlewares:
        try:
            middleware.close()
        except Exception as e:
            logging.error(f"While closing: {e}")