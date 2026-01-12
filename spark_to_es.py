# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit
from itertools import chain

# 1. KHOI TAO SPARK
print(">>> [STEP 1] DANG KHOI TAO SPARK...")
spark = SparkSession.builder.appName("PushToElastic_Fixed").getOrCreate()

# 2. DOC DU LIEU TU HDFS (CA 2 NAM 2023, 2024)
hdfs_path = "hdfs://namenode.hadoop.svc.cluster.local:9000/data/diem_thi/nam=*/*.csv"
print(">>> [STEP 2] DANG DOC DU LIEU TU: " + hdfs_path)

df = spark.read.option("basePath", "hdfs://namenode.hadoop.svc.cluster.local:9000/data/diem_thi/") \
               .csv(hdfs_path, header=True, inferSchema=True)

# In tong so luong ban dau de kiem tra
raw_count = df.count()
print(">>> TONG SO DONG TIM THAY (GOC): " + str(raw_count))

# 3. LAM SACH DU LIEU (DATA CLEANING)
print(">>> [STEP 3] DANG LAM SACH DU LIEU...")

# --- SUA LOI QUAN TRONG O DAY ---
# Phai ket hop ca 'sbd' va 'nam' de tranh xoa nham du lieu giua 2 nam
df_clean = df.dropDuplicates(['sbd', 'nam'])

# In so luong sau khi xoa trung
dedup_count = df_clean.count()
print(">>> SO DONG SAU KHI XOA TRUNG: " + str(dedup_count))
# --------------------------------

# 3.2 Vong lap loc diem ao (> 10)
cac_mon = ["toan", "ngu_van", "ngoai_ngu", "vat_li", "hoa_hoc", "sinh_hoc", "lich_su", "dia_li", "gdcd"]

for mon in cac_mon:
    if mon in df_clean.columns:
        df_clean = df_clean.filter((col(mon) <= 10) | (col(mon).isNull()))

# 4. LAM GIAU DU LIEU (DATA ENRICHMENT)
print(">>> [STEP 4] DANG CHUYEN MA TINH THANH TEN TINH...")

mapping_tinh = {
    1: "Ha Noi", 2: "TP.HCM", 3: "Hai Phong", 4: "Da Nang", 5: "Ha Giang", 6: "Cao Bang",
    7: "Lai Chau", 8: "Lao Cai", 9: "Tuyen Quang", 10: "Lang Son", 11: "Bac Kan",
    12: "Thai Nguyen", 13: "Yen Bai", 14: "Son La", 15: "Phu Tho", 16: "Vinh Phuc",
    17: "Quang Ninh", 18: "Bac Giang", 19: "Bac Ninh", 21: "Hai Duong", 22: "Hung Yen",
    23: "Hoa Binh", 24: "Ha Nam", 25: "Nam Dinh", 26: "Thai Binh", 27: "Ninh Binh",
    28: "Thanh Hoa", 29: "Nghe An", 30: "Ha Tinh", 31: "Quang Binh", 32: "Quang Tri",
    33: "Thua Thien Hue", 34: "Quang Nam", 35: "Quang Ngai", 36: "Kon Tum", 37: "Binh Dinh",
    38: "Gia Lai", 39: "Phu Yen", 40: "Dak Lak", 41: "Khanh Hoa", 42: "Lam Dong",
    43: "Binh Phuoc", 44: "Binh Duong", 45: "Ninh Thuan", 46: "Tay Ninh", 47: "Binh Thuan",
    48: "Dong Nai", 49: "Long An", 50: "Dong Thap", 51: "An Giang", 52: "Ba Ria VT",
    53: "Tien Giang", 54: "Kien Giang", 55: "Can Tho", 56: "Ben Tre", 57: "Vinh Long",
    58: "Tra Vinh", 59: "Soc Trang", 60: "Bac Lieu", 61: "Ca Mau", 62: "Dien Bien",
    63: "Dak Nong", 64: "Hau Giang"
}

mapping_expr = create_map([lit(x) for x in chain(*mapping_tinh.items())])
df_final = df_clean.withColumn("ten_tinh", mapping_expr[col("ma_tinh")])

# 5. GHI VAO ELASTICSEARCH
es_host = "elasticsearch.elastic.svc.cluster.local"
es_port = "9200"
es_index = "diemthi_full/doc"

row_count = df_final.count()
print(">>> [STEP 5] DANG GHI " + str(row_count) + " DONG VAO ELASTICSEARCH...")

df_final.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", es_host) \
    .option("es.port", es_port) \
    .option("es.nodes.wan.only", "true") \
    .mode("overwrite") \
    .save(es_index)

print(">>> [THANH CONG] DU LIEU DA DUOC CAP NHAT TOAN DIEN!")
spark.stop()