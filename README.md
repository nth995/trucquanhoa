# README
## Đăng nhập trên Ubuntu:
```bash
gcloud auth login
```
## Kết nối với cluster:
```bash
gcloud container clusters get-credentials bigdata-cluster --zone asia-southeast1-a --project visual-484003

kubectl get nodes
```
<img width="768" height="97" alt="image" src="https://github.com/user-attachments/assets/c9506c2d-611a-489c-9ce5-ea76306a56f3" />

## Tạo namespace:
```bash
kubectl create namespace hadoop
kubectl create namespace elastic
kubectl create namespace processing

kubectl get namespaces
```
<img width="360" height="249" alt="image" src="https://github.com/user-attachments/assets/68437fdd-ca5b-42f4-bb10-48ae85140b7f" />

## Cài hadoop:
```bash
#Tạo file hadoop.yaml
nano hadoop.yaml

#Cài
kubectl apply -f hadoop.yaml

#Kiểm tra
kubectl get pods -n hadoop
```
<img width="504" height="86" alt="image" src="https://github.com/user-attachments/assets/fa2e33d8-a112-4b04-afae-1fedbc5f6490" />

## Tải dữ liệu vào hadoop
```bash
#Tải dữ liệu từ máy ảo vào pod namenode
kubectl cp folder_2023 hadoop/namenode-fb59bfb8-9b45m:/tmp/folder_2023
kubectl cp folder_2024 hadoop/namenode-fb59bfb8-9b45m:/tmp/folder_2024

#Vào bên trong Pod NameNode
kubectl exec -it -n hadoop namenode-fb59bfb8-9b45m -- /bin/bash

#Tạo thư mục
hdfs dfs -mkdir -p /data/diem_thi/nam=2023
hdfs dfs -mkdir -p /data/diem_thi/nam=2024

#Đẩy file lên HDFS
hdfs dfs -put /tmp/folder_2023/* /data/diem_thi/nam=2023/
hdfs dfs -put /tmp/folder_2024/* /data/diem_thi/nam=2024/

#Kiểm tra
hdfs dfs -ls /data/diem_thi/nam=2023/

#Port-Forward
kubectl port-forward -n hadoop svc/namenode 9870:9870

#Link
http://localhost:9870
```
<img width="763" height="454" alt="image" src="https://github.com/user-attachments/assets/fb7ef242-a74c-4c04-8b95-dc7863824967" />
<img width="1349" height="682" alt="image" src="https://github.com/user-attachments/assets/0fa1a3f2-ecd7-49d6-8072-67315448d604" />

## Cài spack
```bash
#Tạo spark.yaml
nano spark.yaml

#cài
kubectl apply -f spark.yaml

#Kiểm tra
kubectl get pods -n processing
```
<img width="521" height="92" alt="image" src="https://github.com/user-attachments/assets/7f81e7a1-6837-465a-8638-dae15d1ac50d" />

## Cài Elasticsearch + Kibaba
```bash
nano elk.yaml

kubectl apply -f elk.yaml

kubectl get pods -n elastic
```
<img width="523" height="86" alt="image" src="https://github.com/user-attachments/assets/c217ad06-9a16-4e44-8a42-39db27e273a3" />

## Xử lý dữ liệu bằng Spark lưu vào Elasticsearch
```bash
#Tạo file spark_to_es.py
nano spark_to_es.py

#Copy file vào Pod
kubectl cp spark_to_es.py processing/spark-master-7dfdd89c59-4r2m9:/spark_to_es.py

#Chạy lệnh Submit (Tự tải thư viện)
kubectl exec -it -n processing spark-master-7dfdd89c59-4r2m9 -- /spark/bin/spark-submit \
  --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.17.9 \
  /spark_to_es.py
```

#Kiểm tra dữ liệu trong Elaticsearch
```bash
#Mở cổng
kubectl port-forward -n elastic svc/elasticsearch 9200:9200

#Kiểm tra danh sách Index
curl http://localhost:9200/_cat/indices?v

#Xem thử 5 dòng dữ liệu
curl "http://localhost:9200/diemthi_full/_search?pretty&size=5"
```
<img width="961" height="288" alt="image" src="https://github.com/user-attachments/assets/a2f5e3da-4420-472f-8e10-01630610f7bd" />
<img width="375" height="523" alt="image" src="https://github.com/user-attachments/assets/e1d30176-12a3-41f3-a065-26030c465a15" />

## Mở cổng Kibana
```bash
kubectl port-forward -n elastic svc/kibana 5601:5601
```



