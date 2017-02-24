
REPO=docker-dev.hli.io/ccm/celery-worker-dev
TAG=0.0.1

activate:
	source env/bin/activate

deactivate:
	deactivate

run-rabbitmq:
	docker run -itd --name=rabbitmq --net=host rabbitmq:3-management

run-postgres:
	docker run -itd --name=postgress --net=host -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=password library/postgres

run-celery-worker:
	celery -A app.tasks worker --loglevel=INFO --concurrency=10

docker-build:
	docker build -t $(REPO):$(TAG) .

docker-push:
	docker push $(REPO):$(TAG)

marathon:
	./deployment/marathon/upload_group.sh ./deployment/marathon/dev.json

run-worker-local:
	docker run -it --rm --net=host docker-dev.hli.io/ccm/celery-worker-dev:0.0.1 worker

run-python-local:
	docker run -it --rm --net=host docker-dev.hli.io/ccm/celery-worker-dev:0.0.1 ipython
