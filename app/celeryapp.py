
from celery import Celery

app = Celery('tasks',
        broker='amqp://celery_user:password@10.2.95.5:5672/',
        backend='rpc://')
