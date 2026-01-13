# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit, when
from itertools import chain

# 1. KHOI TAO SPARK
print(">>> [STEP 1] DANG KHOI TAO SPARK...")
spark = SparkSession.builder.appName("PushToElastic_Final_Enriched").getOrCreate()

# 2. DOC DU LIEU TU HDFS
hdfs_path = "hdfs://namenode.hadoop.svc.cluster.local:9000/data/diem_thi/nam=*/*.csv"
print(">>> [STEP 2] DANG DOC DU LIEU TU: " + hdfs_path)

df = spark.read.option("basePath", "hdfs://namenode.hadoop.svc.cluster.local:9000/data/diem_thi/") \
               .csv(hdfs_path, header=True, inferSchema=True)

print(">>> TONG SO DONG TIM THAY (GOC): " + str(df.count()))

# 3. LAM SACH DU LIEU
print(">>> [STEP 3] DANG LAM SACH DU LIEU...")

# 3.1 Xoa trung lap (SBD + Nam)
df_clean = df.dropDuplicates(['sbd', 'nam'])
print(">>> SO DONG SAU KHI XOA TRUNG: " + str(df_clean.count()))

# 3.2 Loc diem ao (> 10)
cac_mon = ["toan", "ngu_van", "ngoai_ngu", "vat_li", "hoa_hoc", "sinh_hoc", "lich_su", "dia_li", "gdcd"]
for mon in cac_mon:
    if mon in df_clean.columns:
        df_clean = df_clean.filter((col(mon) <= 10) | (col(mon).isNull()))

# ========================================================
# 4. LAM GIAU DU LIEU (NEW FEATURES)
# ========================================================
print(">>> [STEP 4] DANG TAO COT 'TO HOP' VA 'TEN NGOAI NGU'...")

# --- 4.1: Map Ten Tinh ---
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
map_tinh_expr = create_map([lit(x) for x in chain(*mapping_tinh.items())])
df_final = df_clean.withColumn("ten_tinh", map_tinh_expr[col("ma_tinh")])

# --- 4.2: Xac dinh To Hop Thi (Check cot ton tai truoc khi xu ly) ---
# Ham kiem tra cot co ton tai khong de tranh loi
def has_col(c): return c in df_final.columns

# Mac dinh la False neu cot khong ton tai
c_li = col("vat_li").isNotNull() if has_col("vat_li") else lit(False)
c_hoa = col("hoa_hoc").isNotNull() if has_col("hoa_hoc") else lit(False)
c_sinh = col("sinh_hoc").isNotNull() if has_col("sinh_hoc") else lit(False)

c_su = col("lich_su").isNotNull() if has_col("lich_su") else lit(False)
c_dia = col("dia_li").isNotNull() if has_col("dia_li") else lit(False)
c_gdcd = col("gdcd").isNotNull() if has_col("gdcd") else lit(False)

cond_khtn = c_li & c_hoa & c_sinh
cond_khxh = c_su & c_dia & c_gdcd

df_final = df_final.withColumn("to_hop_thi", 
                               when(cond_khtn, "KHTN")
                               .when(cond_khxh, "KHXH")
                               .otherwise("Khac"))

# --- 4.3: Map Ten Ngoai Ngu ---
mapping_nn = {
    "N1": "Tieng Anh", "N2": "Tieng Nga", "N3": "Tieng Phap", "N4": "Tieng Trung",
    "N5": "Tieng Duc", "N6": "Tieng Nhat", "N7": "Tieng Han"
}
map_nn_expr = create_map([lit(x) for x in chain(*mapping_nn.items())])

# Logic an toan: Kiem tra cot ngoai_ngu co ton tai khong
if "ngoai_ngu" in df_final.columns and "ma_ngoai_ngu" in df_final.columns:
    df_final = df_final.withColumn("ten_ngoai_ngu_temp", map_nn_expr[col("ma_ngoai_ngu")]) \
                       .withColumn("ten_ngoai_ngu", 
                                   when(col("ten_ngoai_ngu_temp").isNotNull(), col("ten_ngoai_ngu_temp"))
                                   .when(col("ngoai_ngu").isNotNull(), "Tieng Anh")
                                   .otherwise(None)) \
                       .drop("ten_ngoai_ngu_temp")
else:
    # Truong hop file thieu cot ngoai ngu
    df_final = df_final.withColumn("ten_ngoai_ngu", lit(None))

# ========================================================

print(">>> CAU TRUC DU LIEU CUOI CUNG:")
df_final.printSchema()

# 5. GHI VAO ELASTICSEARCH
es_host = "elasticsearch.elastic.svc.cluster.local"
es_index = "diemthi_full/doc"

row_count = df_final.count()
print(">>> [STEP 5] DANG GHI " + str(row_count) + " DONG VAO ELASTICSEARCH...")

df_final.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", es_host) \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "true") \
    .mode("overwrite") \
    .save(es_index)

print(">>> [THANH CONG] DU LIEU DA DUOC CAP NHAT!")
spark.stop()
