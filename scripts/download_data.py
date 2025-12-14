import kagglehub
import shutil
import os

# Fungsi bantu untuk memindahkan file
def move_csv(source_folder, dest_name):
    target_folder = "../data"
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    found = False
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if file.endswith(".csv"):
                source_path = os.path.join(root, file)
                dest_path = os.path.join(target_folder, dest_name)
                print(f"Moving {file} -> {dest_path}")
                shutil.move(source_path, dest_path)
                found = True
                break # Ambil satu file csv utama saja
        if found: break
def move_csv_from_subfolder(source_folder, subfolder, dest_name):
    target_folder = "../data"
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    sub_path = os.path.join(source_folder, subfolder)
    if not os.path.exists(sub_path):
        print(f"Subfolder '{subfolder}' tidak ditemukan di {source_folder}")
        return

    found = False
    for root, dirs, files in os.walk(sub_path):
        for file in files:
            if file.endswith(".csv"):
                source_path = os.path.join(root, file)
                dest_path = os.path.join(target_folder, dest_name)
                print(f"Moving {file} -> {dest_path}")
                shutil.move(source_path, dest_path)
                found = True
                break
        if found:
            break

    if not found:
        print(f"Tidak ditemukan file CSV dalam folder '{subfolder}'.")

print("--- 1. Downloading Superstore Dataset ---")
path_store = kagglehub.dataset_download("vivek468/superstore-dataset-final")
move_csv(path_store, "superstore.csv")



print("\n--- 2. Downloading Twitter Dataset (Size Besar ~200MB+) ---")
path_twitter = kagglehub.dataset_download("thoughtvector/customer-support-on-twitter")

# Ambil CSV dari folder twcs/
move_csv_from_subfolder(path_twitter, "twcs", "tweets.csv")

