import os
from celeryapp import app
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from time import sleep

master='mesos://zk://10.2.95.5:2181/qs-dmitry-dgqt'
spark_image='docker-dev.hli.io/ccm/hli-rspark:2.0.0'
appName='worker-01'

@app.task
def add_task(x,y):
    return x + y

def create_spark_context(cores, ram):
    #os.environ['PYSPARK_SUBMIT_ARGS'] = ' --packages com.amazonaws:aws-java-sdk:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 pyspark-shell'
    os.environ['PYSPARK_SUBMIT_ARGS'] = ' --conf spark.executor.extraClassPath=/usr/local/spark/extra_jars/* --conf spark.driver.extraClassPath=/usr/local/spark/extra_jars/* pyspark-shell'

    conf = SparkConf()\
        .setAppName(appName)\
        .setMaster(master)\
        .set('spark.driver.maxResultSize','1g')\
        .set('spark.driver.memory','1g')\
        .set('spark.driver.cores','1')\
        .set('spark.driver.extraClassPath','/usr/local/spark/extra_jars/*')\
        .set('spark.mesos.coarse','true')\
        .set('spark.mesos.executor.docker.image', spark_image)\
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
        .set('spark.kryoserializer.buffer.max','1g')\
        .set('spark.sql.shuffle.partitions','200')\
        .set('spark.shuffle.spill','true')

    sc = SparkContext(conf=conf)
    hadoop_config = sc._jsc.hadoopConfiguration()
    hadoop_config.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    hadoop_config.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")

    return sc

@app.task
def spark_sum_num_task(n, cores=4, ram='16g'):
    sc = create_spark_context(cores, ram)
    try:
        res = sc.parallelize(range(n)).sum()
        return res
    except Exception as exc:
        raise exc
    finally:
        sc.stop()

@app.task
def spark_join_cohorts_task(cohort1_path, cohort2_path, result_path, cores=16, ram='50g'):
    sc = create_spark_context(cores, ram)
    try:
        sqlCtx = SQLContext(sc)

        rdd1 = sqlCtx.read.parquet(cohort1_path)
        rdd1.registerTempTable('rdd1')

        rdd2 = sqlCtx.read.parquet(cohort2_path)
        rdd2.registerTempTable('rdd2')

        sql = """
        SELECT
          CASE WHEN rdd1.annotation_hash IS NOT NULL THEN rdd1.annotation_hash ELSE rdd2.annotation_hash END AS annotation_hash,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.chrom ELSE rdd2.chrom END AS chrom,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.pos ELSE rdd2.pos END AS pos,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.ref ELSE rdd2.ref END AS ref,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.alt ELSE rdd2.alt END AS alt,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.highest_clinical_significance ELSE rdd2.highest_clinical_significance END AS highest_clinical_significance,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.rs_id ELSE rdd2.rs_id END AS rs_id,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.gene_name ELSE rdd2.gene_name END AS gene_name,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.annotation ELSE rdd2.annotation END AS annotation,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.clinvar_trait_names ELSE rdd2.clinvar_trait_names END AS clinvar_trait_names,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.hgvs_c ELSE rdd2.hgvs_c END AS hgvs_c,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.hgvs_p ELSE rdd2.hgvs_p END AS hgvs_p,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.cadd_score ELSE rdd2.cadd_score END AS cadd_score,
          rdd1.cohort_allele_frequency AS af1,
          rdd2.cohort_allele_frequency AS af2,
          rdd1.hli_ancestry_allele_frequency AS ancestry_af1,
          rdd2.hli_ancestry_allele_frequency AS ancestry_af2,
          CASE
            WHEN rdd1.cohort_allele_frequency IS NULL THEN -rdd2.cohort_allele_frequency
            WHEN rdd2.cohort_allele_frequency IS NULL THEN rdd1.cohort_allele_frequency
            ELSE rdd1.cohort_allele_frequency - rdd2.cohort_allele_frequency
          END AS cohort_delta_af,
          CASE
            WHEN rdd1.hli_ancestry_allele_frequency IS NULL THEN -rdd2.hli_ancestry_allele_frequency
            WHEN rdd2.hli_ancestry_allele_frequency IS NULL THEN rdd1.hli_ancestry_allele_frequency
            ELSE rdd1.hli_ancestry_allele_frequency - rdd2.hli_ancestry_allele_frequency
          END AS ancestry_delta_af,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.variant_type ELSE rdd2.variant_type END AS variant_type,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.hli_allele_frequency ELSE rdd2.hli_allele_frequency END AS hli_allele_frequency,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.high_conf_region ELSE rdd2.high_conf_region END AS high_conf_region,
          CASE
            WHEN rdd1.chrom IS NOT NULL AND rdd2.chrom IS NOT NULL THEN 'ALL'
            WHEN rdd1.chrom IS NOT NULL AND rdd2.chrom IS NULL THEN 'COHORT1'
            ELSE 'COHORT2'
          END AS cohort_origin,
          CASE WHEN rdd1.chrom IS NOT NULL THEN rdd1.sample_count ELSE rdd2.sample_count END AS sample_count
        FROM rdd1 FULL OUTER JOIN rdd2 ON (
            rdd1.annotation_hash = rdd2.annotation_hash
        )
        """

        rdd_result = sqlCtx.sql(sql)
        rdd_result.persist()
        rdd_result.write.format("parquet").mode("overwrite").save(result_path)
        res = rdd_result.count()
        #rdd_result.write.parquet(str(result_path))
        return res

    except Exception as exc:
        raise exc
    finally:
        sc.stop()

