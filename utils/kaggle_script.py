import shutil
import os

import kagglehub

src_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")

dst_path = "../data_raw"

os.makedirs(dst_path, exist_ok=True)

for file in os.listdir(src_path):
    if file.endswith(".csv"):
        shutil.copy(os.path.join(src_path, file), dst_path)
        

