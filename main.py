import time
import csv
import os
import shutil
from supabase import create_client
from dotenv import load_dotenv
import aiohttp
import asyncio

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)
folder_path = os.getenv("FILES_FOLDER")
policies = []
filename = 'broken_url.csv'
file_exists = os.path.isfile(filename)

#Moves files processed to another folder   
def move_to_folder(file_path, move_path, retries=3, delay=1):
    try:
        shutil.move(file_path, move_path)
    except PermissionError as e:
        if retries > 0:
            print(f"File in use, retrying... ({retries} retries left)")
            time.sleep(delay)
            move_to_folder(file_path, move_path, retries - 1, delay)
        else:
            print(f"Error moving file: {e}")
    except Exception as e:
        print(f"Error moving file: {e}")

#checks if the Url is a working urL
async def url_works(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                return response.status == 200
    except Exception as e:
        print(e)
        return False
    
    
async def main():
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            
            move_path = os.path.join(os.getenv("MOVED_TO"), filename)
            
            with open(file_path, "r", newline='') as csv_f:
                csv_r = csv.reader(csv_f)
                
                csv_r = list(csv_r)
                
                headers = [h.strip().replace(" ", "_").lower() for h in csv_r[1]] # turns all headers to lowercase and replces space with underscore (to match databse)
                
                data = csv_r[2:-1] #cuts list to 3 line and seceond to last line ( to avoid the table header)
                
                print(f"\n\nContents of {filename}:")
                for row in data:
                    policy = dict(zip(headers, row))
                    #if url works, it adds to policy list 
                    if await url_works(policy['url']):
                        policies.append(policy)
                    else:
                        with open('broken_urls.csv', mode='w', newline='', encoding='utf-8') as file:
                            writer = csv.DictWriter(file, fieldnames=headers)
                            #if file didnt exist before, it defines the headers
                            if not file_exists:
                                writer.writeheader()
                            
                            writer.writerow(policy)
                            
                
            move_to_folder(file_path, move_path)
        


        
              
#response = supabase.table("policies").insert(policies).execute()
asyncio.run(main())


