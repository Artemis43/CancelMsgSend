import time
import pandas as pd
import os
from pyrogram import Client
from pyrogram.errors import FloodWait, PeerIdInvalid
from keep_alive import keep_alive

keep_alive()

# Define your variables
api_id = os.environ.get('Id')
api_hash = os.environ.get('Hash')
session_name = os.environ.get('Session')
group_username = os.environ.get('Destination')  # or group invite link
log_chat_username = os.environ.get('Log')  # or log chat invite link
interval = os.environ.get('Interval')  # Interval in seconds
Prefix = os.environ.get('Prefix') # eg: /ql or /ql6...

# Check for None values
if None in (api_id, api_hash, session_name, group_username, log_chat_username, interval, Prefix):
    print("One or more required environment variables are missing.")
    exit(1)

# Convert interval to integer
interval = int(interval)

# Read the CSV file
df = pd.read_csv('match_names.csv')

# Initialize the Pyrogram Client
app = Client(session_name, api_id, api_hash)

# Function to get chat ID from username or invite link
def get_chat_id(username):
    try:
        chat = app.get_chat(username)
        return chat.id
    except PeerIdInvalid:
        print(f"Invalid username or invite link: {username}")
        return None

# Function to send magnet links
def send_magnet_links():
    log_data = []

    try:
        app.start()

        group_id = get_chat_id(group_username)
        log_chat_id = get_chat_id(log_chat_username)

        if group_id is None or log_chat_id is None:
            print("Unable to fetch group or log chat ID. Exiting.")
            return

        for index, row in df.iterrows():
            game_name = row['game_name']
            repack_size = row['repack_size']
            magnet_link = row['magnet_link']

            message = f"{Prefix} {magnet_link}"

            try:
                app.send_message(group_id, message)
                sent_time = pd.Timestamp.now()
                log_data.append([sent_time, game_name, group_id])
                print(f"Sent message for {game_name} at {sent_time}")
            except FloodWait as e:
                print(f"Flood wait for {e.x} seconds.")
                time.sleep(e.x)
                app.send_message(group_id, message)
                sent_time = pd.Timestamp.now()
                log_data.append([sent_time, game_name, group_id])
                print(f"Sent message for {game_name} at {sent_time}")

            time.sleep(interval)

        # Create the log CSV
        log_df = pd.DataFrame(log_data, columns=['sent_time', 'game_name', 'destination_chat'])
        log_df.to_csv('sent_log.csv', index=False)

        # Send the log CSV to the log chat
        app.send_document(log_chat_id, 'sent_log.csv')
        print(f"Log file sent to chat {log_chat_id}")

    finally:
        app.stop()

if __name__ == "__main__":
    send_magnet_links()
