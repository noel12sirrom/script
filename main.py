import csv
import os
import shutil
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)


folder_path = os.getenv("FILES_FOLDER")

policies = [
]

for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)
        
        move_path = os.path.join(os.getenv("MOVED_TO"), filename)
        
        with open(file_path, "r", newline='') as csv_f:
            csv_r = csv.reader(csv_f)
            
            csv_r = list(csv_r)
            
            headers = [h.strip().replace(" ", "_").lower() for h in csv_r[1]] # turns all headers to lowercase and replces space with underscore (to match databse)
            print(headers)
            data = csv_r[2:-1] #cuts list to 3 line and seceond to last line ( to avoid the table header)
            
            print(f"\n\nContents of {filename}:")
            for row in data:
                policy = dict(zip(headers, row))
                policies.append(policy)
        
        #Moves files processed to another folder   
        try:
            shutil.move(file_path, move_path)
        except Exception as e:
            print(f"Error moving file: {e}")
              
response = supabase.table("policies").insert(policies).execute()
print(response)
