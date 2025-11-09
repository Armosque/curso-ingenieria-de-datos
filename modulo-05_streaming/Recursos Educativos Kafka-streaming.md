# Repos para aprender **Apache Kafka** y construir **pipelines de streaming**

Este doc reúne repositorios prácticos para aprender Kafka desde cero y montar pipelines de datos de punta a punta (producers/consumers, stream processing, CDC con Kafka Connect/Debezium, UIs de observabilidad, y despliegues en AWS/MSK). Incluye ejemplos de **ML en streaming** (Kafka Streams y TensorFlow/MQTT).


## Setup rápido (Docker Compose)

```bash
# 1) Clona el stack de Confluent
git clone https://github.com/confluentinc/cp-all-in-one.git
cd cp-all-in-one/cp-all-in-one

# 2) Levanta todos los servicios
docker compose -f docker-compose.yml up -d

# 3) Verifica contenedores
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### Crear un topic y producir/consumir (CLI)

> Ajusta el host/puerto según tu compose (KRaft/PLAINTEXT o SASL_SSL si activas seguridad).

```bash
# Crear un topic
kafka-topics --bootstrap-server localhost:9092 --create --topic demo.events --partitions 3 --replication-factor 1

# Producer (stdin)
kafka-console-producer --bootstrap-server localhost:9092 --topic demo.events

# Consumer\ nkafka-console-consumer --bootstrap-server localhost:9092 --topic demo.events --from-beginning
```


## Stream processing

### A) Kafka Streams 

  * [confluentinc/examples](https://github.com/confluentinc/examples)
  * [kaiwaehner/kafka-streams-machine-learning-examples](https://github.com/kaiwaehner/kafka-streams-machine-learning-examples)

### B) Flink SQL

* Curso/ejercicios: [apache/flink-training](https://github.com/apache/flink-training)

### C) Spark Structured Streaming

* Ver ejemplos en [apache/spark](https://github.com/apache/spark) (docs/`examples/src/main`)


## CDC con Debezium / Kafka Connect

* **Ejecuta ejemplos**: [debezium/debezium-examples](https://github.com/debezium/debezium-examples)
* **Datos sintéticos**: [confluentinc/kafka-connect-datagen](https://github.com/confluentinc/kafka-connect-datagen)


## UIs para observar y depurar

* [provectus/kafka-ui](https://github.com/provectus/kafka-ui)
* [redpanda-data/console](https://github.com/redpanda-data/console)

En ambas puedes ver: topics, mensajes, keys, consumer lag, offsets, y probar produce/consume.


## AWS / MSK y Flink gestionado

* **MSK + Kafka Streams**: [aws-samples/amazon-msk-with-apache-kafka-streams-api](https://github.com/aws-samples/amazon-msk-with-apache-kafka-streams-api)
* **Managed Flink (Kinesis/MSK)**: [aws-samples/amazon-managed-service-for-apache-flink-examples](https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples)


## ML + IoT en streaming

* **IoT/MQTT + TensorFlow + Kafka**: [kaiwaehner/hivemq-mqtt-tensorflow-kafka-realtime-iot-machine-learning-training-inference](https://github.com/kaiwaehner/hivemq-mqtt-tensorflow-kafka-realtime-iot-machine-learning-training-inference)

  * Pipeline endd‑a‑end: ingestión MQTT (HiveMQ) - Kafka - entrenamiento/inferencia TF.
* **Kafka Streams + ML**: [kaiwaehner/kafka-streams-machine-learning-examples](https://github.com/kaiwaehner/kafka-streams-machine-learning-examples)

  * Patrones de feature engineering y scoring embebidos en topologías Kafka Streams.

---


