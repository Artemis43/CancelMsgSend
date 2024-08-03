import time
import pandas as pd
import os
import csv
import asyncio
import requests
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, PeerIdInvalid, SessionPasswordNeeded
from keep_alive import keep_alive
#from dotenv import load_dotenv
import sqlite3

# Load environment variables from .env file
#load_dotenv()

keep_alive()

# Define your variables
api_id = os.getenv('ApiId') #Both
api_hash = os.getenv('ApiHash') #Both
session_url = os.getenv('SessionUrl')  # URL to download the session file #Both
group_username = os.getenv('DestinationUsername')  # or group invite link #LinkSend
log_chat_username = os.getenv('LogsInChat')  # or log chat invite link #LinkSend
interval = int(os.getenv('IntervalBetweenLeech', 60))  # Interval in seconds, default to 60 seconds #LinkSend
Prefix = os.getenv('PrefixForBot')  # e.g., /ql or /ql6... #LinkSend
csv_url = os.getenv('CsvUrlForMagnetLinks')  # URL of the CSV file
user_id = os.getenv('UserIdForRegexMatch')  # Your user ID #Cancelmsg
bot_username = os.getenv('BotToBeMonitoredNoAt')  # Bot's username without '@' #Cancelmsg
group_id = os.getenv('GroupToBeMonitoredNoAt')  # Username of the group without '@' #Cancelmsg
cancel_interval = int(os.getenv('CancelMessageInterval', 10))  # Default to 10 seconds #Cancelmsg

# Download the CSV file
csv_file_path = 'match_games.csv'
try:
    response = requests.get(csv_url)
    response.raise_for_status()
    with open(csv_file_path, 'wb') as f:
        f.write(response.content)
    print(f"CSV file downloaded and saved as {csv_file_path}")
except requests.exceptions.RequestException as e:
    print(f"Failed to download CSV file: {e}")
    exit(1)

# Verify the CSV file exists and can be read
try:
    df = pd.read_csv(csv_file_path)
    print("CSV file loaded successfully.")
except Exception as e:
    print(f"Failed to read CSV file: {e}")
    exit(1)

# Download the session file
session_file = 'send.session'
try:
    session_response = requests.get(session_url)
    session_response.raise_for_status()
    with open(session_file, 'wb') as f:
        f.write(session_response.content)
    print(f"Session file downloaded and saved as {session_file}")

    # Check the size of the session file
    if os.path.getsize(session_file) == 0:
        print("Downloaded session file is empty.")
        exit(1)

    # Print the first few bytes of the session file
    with open(session_file, 'rb') as f:
        content = f.read(10)
        print(f"First 10 bytes of the session file: {content}")

except requests.exceptions.RequestException as e:
    print(f"Failed to download session file: {e}")
    exit(1)

# Ensure the session file exists
if not os.path.exists(session_file):
    print(f"Session file {session_file} does not exist.")
    exit(1)

# Verify the session file content
try:
    conn = sqlite3.connect(session_file)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"Tables in session file: {tables}")
    conn.close()
except sqlite3.Error as e:
    print(f"Failed to read session file: {e}")
    exit(1)

session = "send"
# Initialize the Pyrogram Client with session file
app = Client(session, api_id=api_id, api_hash=api_hash)

# Function to save the line to CSV with UTF-8 encoding
def save_line_to_csv(line):
    with open('gids.csv', 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([line])
    extract_and_save_gid(line)

# Function to extract the desired part and store it in a new row
def extract_and_save_gid(line):
    try:
        # Extract the part after the l in /btsel
        extracted_part = line.split('l')[-1]
        # Remove any leading or trailing spaces
        extracted_part = extracted_part.strip()
        # Save the extracted part to CSV
        with open('gids_extracted.csv', 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([extracted_part])
        clear_gids_csv()
        # Schedule the message to be sent
        asyncio.create_task(send_custom_message(extracted_part))
    except IndexError:
        print("The line does not contain an underscore.")

# Function to clear the contents of gids.csv
def clear_gids_csv():
    with open('gids.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csvfile.truncate()
    print("gids.csv has been cleared.")

# Function to clear the contents of gids_extracted.csv
def clear_gids_extracted_csv():
    with open('gids_extracted.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csvfile.truncate()
    print("gids_extracted.csv has been cleared.")

# Function to send a custom message after a specific interval
async def send_custom_message(extracted_part):
    await asyncio.sleep(cancel_interval)  # Wait for 10 seconds (adjust the interval as needed)
    custom_message = f"/cancel{extracted_part}@{bot_username}"
    await app.send_message(group_id, custom_message)
    clear_gids_extracted_csv()
    print(f"Sent message: {custom_message}")

@app.on_message(filters.chat(group_id))
async def handler(client, message):
    message_text = message.text
    # Split the message into lines
    lines = message_text.split('\n')
    
    # Iterate over the lines
    for i, line in enumerate(lines):
        if f"ID: {user_id}" in line:
            if i + 1 < len(lines):  # Ensure there is a line for the GID
                gid_line = lines[i + 1]
                save_line_to_csv(gid_line)

# Function to get chat ID from username or invite link
async def get_chat_id(username):
    try:
        chat = await app.get_chat(username)
        return chat.id
    except PeerIdInvalid:
        print(f"Invalid username or invite link: {username}")
        return None

# Function to send magnet links
async def send_magnet_links():
    log_data = []

    try:
        await app.start()
        print("Client started successfully.")

        group_id2 = group_username
        log_chat_id = await get_chat_id(log_chat_username)

        if group_id2 is None or log_chat_id is None:
            print("Unable to fetch group or log chat ID. Exiting.")
            return

        for index, row in df.iterrows():
            game_name = row['game_name']
            repack_size = row['repack_size']
            magnet_link = row['magnet_link']

            message = f"{Prefix} {magnet_link}"

            try:
                await app.send_message(group_id2, message)
                sent_time = pd.Timestamp.now()
                log_data.append([sent_time, game_name, group_id2])
                print(f"Sent message for {game_name} at {sent_time}")
            except FloodWait as e:
                print(f"Flood wait for {e.x} seconds.")
                await asyncio.sleep(e.x)
                await app.send_message(group_id2, message)
                sent_time = pd.Timestamp.now()
                log_data.append([sent_time, game_name, group_id2])
                print(f"Sent message for {game_name} at {sent_time}")

            await asyncio.sleep(interval)

        # Create the log CSV
        log_df = pd.DataFrame(log_data, columns=['sent_time', 'game_name', 'destination_chat'])
        log_df.to_csv('sent_log.csv', index=False)

        # Send the log CSV to the log chat
        await app.send_document(log_chat_id, 'sent_log.csv')
        print(f"Log file sent to chat {log_chat_id}")

    except SessionPasswordNeeded:
        print("Two-step verification is enabled. Please provide the password.")
    finally:
        await app.stop()
        print("Client stopped.")

if __name__ == "__main__":
    app.run(send_magnet_links())
