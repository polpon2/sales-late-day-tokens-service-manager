import pika, sys, os, time, json
from dotenv import load_dotenv
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.export import (BatchSpanProcessor,
                                            ConsoleSpanExporter)

load_dotenv()

provider = TracerProvider()
processor = BatchSpanProcessor(ConsoleSpanExporter())
provider.add_span_processor(processor)

# Sets the global default tracer provider
trace.set_tracer_provider(provider)

# Creates a tracer from the global tracer provider
tracer = trace.get_tracer(__name__)

def request_complete(file: str):
    # url = os.getenv('COMPLETED_ENDPOINT')


    # try:
    #     requests.post(url, json = json.loads(file))
    # except: # Set default
    #     requests.post("http://localhost:8000/api/process-completed", json = json.loads(file))
    return True


def callback_backend(ch, method, properties, body):
    body_dict: dict = json.loads(body)
    body_dict["stage"] = 0

    print(f" [x] Received {body} from backend")
    ch.queue_declare(queue='to.order', arguments={
                          'x-message-ttl' : 1000,
                          'x-dead-letter-exchange' : 'dlx',
                          'x-dead-letter-routing-key' : 'dl'
                          })

    ch.basic_publish(exchange='',
                        routing_key='to.order',
                        body=json.dumps(body_dict))

    print(f" [x] Sent {body_dict} to order")
    return


def callback_order(ch, method, properties, body):
    body_dict: dict = json.loads(body)
    body_dict["stage"] = 1

    print(f" [x] Received {body} from order")
    ch.queue_declare(queue='to.payment', arguments={
                          'x-message-ttl' : 1000,
                          'x-dead-letter-exchange' : 'dlx',
                          'x-dead-letter-routing-key' : 'dl'
                          })

    ch.basic_publish(exchange='',
                        routing_key='to.payment',
                        body=json.dumps(body_dict))

    print(f" [x] Sent {body_dict} to payment")
    return


def callback_payment(ch, method, properties, body):
    body_dict: dict = json.loads(body)
    body_dict["stage"] = 2

    print(f" [x] Received {body} from payment")
    ch.queue_declare(queue='to.inventory', arguments={
                          'x-message-ttl' : 1000,
                          'x-dead-letter-exchange' : 'dlx',
                          'x-dead-letter-routing-key' : 'dl'
                          })

    ch.basic_publish(exchange='',
                        routing_key='to.inventory',
                        body=json.dumps(body_dict))

    print(f" [x] Sent {body_dict} to inventory")
    return


def callback_inventory(ch, method, properties, body):
    body_dict: dict = json.loads(body)
    body_dict["stage"] = 3

    print(f" [x] Received {body} from inventory")
    ch.queue_declare(queue='to.deliver', arguments={
                          'x-message-ttl' : 1000,
                          'x-dead-letter-exchange' : 'dlx',
                          'x-dead-letter-routing-key' : 'dl'
                          })

    ch.basic_publish(exchange='',
                        routing_key='to.deliver',
                        body=json.dumps(body_dict))

    print(f" [x] Sent {body_dict} to deliver")
    return

def callback_deliver(ch, method, properties, body):
    body_dict: dict = json.loads(body)
    body_dict["stage"] = 4

    print(f" [x] Received {body} from inventory")
    ch.queue_declare(queue='to.order.complete')

    ch.basic_publish(exchange='',
                        routing_key='to.order.complete',
                        body=json.dumps(body_dict))

    request_complete(body)
    print(f" [x] Process {body} completed...")
    return

def main():
    print("Attempting to connect to Rabbit")
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbit-mq', port=5672))
    except:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

    channel = connection.channel()

    channel.queue_declare(queue='from.backend')
    channel.queue_declare(queue='from.order')
    channel.queue_declare(queue='from.payment')
    channel.queue_declare(queue='from.inventory')
    channel.queue_declare(queue='from.deliver')

    channel.basic_consume(queue='from.backend', on_message_callback=callback_backend, auto_ack=True)
    channel.basic_consume(queue='from.order', on_message_callback=callback_order, auto_ack=True)
    channel.basic_consume(queue='from.payment', on_message_callback=callback_payment, auto_ack=True)
    channel.basic_consume(queue='from.inventory', on_message_callback=callback_inventory, auto_ack=True)
    channel.basic_consume(queue='from.deliver', on_message_callback=callback_deliver, auto_ack=True)

    # Init deadletter exchange/queue

    channel.exchange_declare(exchange='dlx', exchange_type='direct')

    channel.queue_declare(queue='dl',
                        arguments={
                                'x-dead-letter-exchange': 'amq.direct',
                        })

    channel.queue_bind(exchange='dlx', queue='dl')

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    except Exception as e:
        print(f"Exception with error {e}")
        time.sleep(5)