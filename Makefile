all:

topics:
	${CONFLUENT_HOME}/bin/kafka-topics --bootstrap-server localhost:9092 --create \
		--topic "steps" \
		--partitions 1 --replication-factor 1 \
		--if-not-exists
	${CONFLUENT_HOME}/bin/kafka-topics --bootstrap-server localhost:9092 --create \
		--topic "activity-points" \
		--partitions 1 --replication-factor 1 \
		--if-not-exists
	${CONFLUENT_HOME}/bin/kafka-topics --bootstrap-server localhost:9092 --create \
		--topic "policies" \
		--partitions 1 --replication-factor 1 \
		--if-not-exists
