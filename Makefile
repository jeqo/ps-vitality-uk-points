all:

topics:
	${CONFLUENT_HOME}/bin/kafka-topics --bootstrap-server localhost:9092 --create \
		--topic "steps" \
		--partitions 1 --replication-factor 1
	${CONFLUENT_HOME}/bin/kafka-topics --bootstrap-server localhost:9092 --create \
		--topic "activity-points" \
		--partitions 1 --replication-factor 1
