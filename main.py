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
broken_filename = 'broken_url.csv'
move_folder = os.getenv("MOVED_TO")



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
async def url_works(url, session):
    try:
        async with session.get(url, timeout=10) as response:
            return response.status == 200
    except Exception as e:
        print(f"URL error ({url}): {e}")
        return False
    
    
async def process_file(filename, session):
    policies = []
    file_path = os.path.join(folder_path, filename)
    move_path = os.path.join(move_folder, filename)
    
    try:   
        with open(file_path, "r", newline='', encoding='utf-8') as csv_f:
            csv_r = list(csv.reader(csv_f))
        
        
            headers = [h.strip().replace(" ", "_").lower() for h in csv_r[1]] # turns all headers to lowercase and replces space with underscore (to match databse)
            
            data = csv_r[2:-1] #cuts list to 3 line and seceond to last line ( to avoid the table header)
            
            tasks = []
            for row in data:
                if len(row) != len(headers):
                    print(f"row items lenght doesnt match up to header {filename}")
                    continue
                policy = dict(zip(headers, row))
                tasks.append((policy, headers))
                
            # Run all URL checks concurrently
            check_tasks = [
                validate_and_collect(policy, headers, session)
                for policy, headers in tasks
            ]
            results = await asyncio.gather(*check_tasks)

            for result in results:
                if result:
                    policies.append(result)
    finally:
        move_to_folder(file_path, move_path)
    
    return policies
            
  
# Validate URL and return policy if it works
async def validate_and_collect(policy, headers, session):
    if await url_works(policy.get('url', ''), session):
        return policy
    else:
        write_broken_url(policy, headers)
        return None    

# Write broken URLs to a CSV
def write_broken_url(policy, headers):
    file_exists = os.path.isfile(broken_filename)
    with open(broken_filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow(policy)   
        
        
async def main():
    policies_to_insert = []
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36"
}
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = []
        for filename in os.listdir(folder_path):
            if filename.endswith(".csv"):
                tasks.append(process_file(filename, session))

        all_results = await asyncio.gather(*tasks)

        # Flatten results
        for batch in all_results:
            policies_to_insert.extend(batch)
    
    if policies_to_insert:
        try:
            response = supabase.table("policies").insert(policies_to_insert).execute()
            print(f"Inserted {len(policies_to_insert)} policies to Supabase.")
        except Exception as e:
            print(f"Error inserting into Supabase: {e}")    
    
    
     
              
if __name__ == "__main__":
    asyncio.run(main())


