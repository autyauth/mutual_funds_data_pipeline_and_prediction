import csv
import pandas as pd
def read_csv_with_auto_delimiter(file_path):
    """
    อ่านไฟล์ CSV โดยตรวจสอบตัวคั่นข้อมูลอัตโนมัติ (`,` หรือ `\t`)
    """
    with open(file_path, 'r') as file:
        # อ่านตัวอย่างข้อมูล
        sample = file.read(2048)  # เพิ่ม sample size
        file.seek(0)  # รีเซ็ตตำแหน่งของไฟล์
        
        # ตรวจจับ delimiter
        sniffer = csv.Sniffer()
        try:
            delimiter = sniffer.sniff(sample).delimiter
        except csv.Error:
            print("Could not determine delimiter, defaulting to '\\t'")
            delimiter = '\t'  # ตั้งค่า default delimiter เป็น '\t'

    # โหลดไฟล์ด้วยตัวคั่นข้อมูลที่ตรวจพบหรือค่า default
    data = pd.read_csv(file_path, delimiter=delimiter)
    return data