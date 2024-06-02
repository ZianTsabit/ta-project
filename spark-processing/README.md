spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --master local[*] clients/process-clients.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --master local[*] employees/process-employees.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --master local[*] offices/process-offices.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --master local[*] orderdetails/process-orderdetails.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --master local[*] orders/process-orders.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --master local[*] payments/process-payments.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --master local[*] productlines/process-productlines.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --master local[*] products/process-products.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --master local[*] main.py
    --kafka-bootstrap-servers localhost:9092
    --kafka-topic dbserver1.db_shop.offices
    --lakehouse-host localhost
    --lakehouse-port 5434
    --lakehouse-db mydb
    --lakehouse-user user
    --lakehouse-password password
