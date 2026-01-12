# trucquanhoa
Đăng nhập trên Ubuntu:
```bash
gcloud auth login
```
Kết nối với cluster:
```bash
gcloud container clusters get-credentials bigdata-cluster --zone asia-southeast1-a --project visual-484003

kubectl get nodes
```
<img width="768" height="97" alt="image" src="https://github.com/user-attachments/assets/c9506c2d-611a-489c-9ce5-ea76306a56f3" />

Tạo namespace:
```bash
kubectl create namespace hadoop
kubectl create namespace elastic
kubectl create namespace processing

kubectl get namespaces
```
<img width="360" height="249" alt="image" src="https://github.com/user-attachments/assets/68437fdd-ca5b-42f4-bb10-48ae85140b7f" />

Cài hadoop:
```bash
#Tạo file hadoop.yaml
nano hadoop.yaml

#Cài
kubectl apply -f hadoop.yaml

#Kiểm tra
kubectl get pods -n hadoop
```
<img width="504" height="86" alt="image" src="https://github.com/user-attachments/assets/fa2e33d8-a112-4b04-afae-1fedbc5f6490" />

Tải dữ liệu vào hadoop
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





