"""Kafka connection options, shared by the producer and both consumers.

Bootstrap servers come from a job parameter; optional SASL credentials are read at
runtime from a Databricks secret scope (never committed). Imports of `dbutils` are
lazy so this module still imports off-cluster (e.g. in local tests)."""
from typing import Dict, Optional


def kafka_options(bootstrap: str, secret_scope: Optional[str] = None, spark=None) -> Dict[str, str]:
    opts = {"kafka.bootstrap.servers": bootstrap}
    if secret_scope:
        jaas = _secret(spark, secret_scope, "sasl_jaas_config")
        if jaas:
            # Mechanism defaults to PLAIN (back-compat); set the optional
            # `sasl_mechanism` scope key for other schemes, e.g. SCRAM-SHA-512
            # (Amazon MSK SASL/SCRAM, Confluent Cloud SCRAM). The matching login
            # module goes in `sasl_jaas_config`; on Databricks the Kafka connector
            # is shaded, so SCRAM uses
            # kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule.
            mechanism = _secret(spark, secret_scope, "sasl_mechanism") or "PLAIN"
            opts.update({
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": mechanism,
                "kafka.sasl.jaas.config": jaas,
            })
    return opts


def _secret(spark, scope: str, key: str) -> Optional[str]:
    try:
        from pyspark.dbutils import DBUtils  # available only on Databricks
        return DBUtils(spark).secrets.get(scope, key)
    except Exception:
        return None  # no SASL configured / not on a cluster → plaintext
