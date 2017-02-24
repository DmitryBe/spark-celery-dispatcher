from celeryapp import app
from pyspark import SparkContext, SparkConf
from time import sleep

master='mesos://zk://10.2.95.5:2181/qs-dmitry-dgqt'
spark_image='docker-dev.hli.io/ccm/hli-rspark-plink:2.0.1'
cores='32'
ram='50g'
appName='worker-01'

@app.task
def add_task(x,y):
    return x + y

def create_spark_context():
    conf = SparkConf()\
        .setAppName(appName)\
        .setMaster(master)\
        .set('spark.driver.maxResultSize','1g')\
        .set('spark.driver.memory','1g')\
        .set('spark.driver.cores','1')\
        .set('spark.driver.extraClassPath','/usr/local/spark/extra_jars/*')\
        .set('spark.mesos.coarse','true')\
        .set('spark.mesos.executor.docker.image',spark_image)\
        .set('spark.mesos.executor.home','/usr/local/spark')\
        .set('spark.mesos.executor.docker.volumes','/docker:/docker')\
        .set('spark.executor.extraClassPath','/usr/local/spark/extra_jars/*')\
        .set('spark.task.maxFailures','100')\
        .set('spark.task.cpus','1')\
        .set('spark.executor.memory',ram)\
        .set('spark.cores.max',cores)\
        .set('spark.executor.heartbeatInterval','10')\
        .set('spark.sql.parquet.compression.codec','gzip')\
        .set('spark.sql.warehouse.dir','file:///tmp')\
        .set('spark.serializer','org.apache.spark.serializer.KryoSerializer')\
        .set('spark.sql.shuffle.partitions','200')\
        .set('spark.shuffle.spill','true')

    sc = SparkContext(conf=conf)
    return sc

@app.task
def spark_sum_task(n):
    sc = create_spark_context()
    try:
        res = sc.parallelize(range(n)).sum()
        return res
    except Exception as exc:
        raise exc
    finally:
        sc.stop()
