D:\kafka\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 2 --topic hadoop-sink  --config min.insync.replicas=2