import os
import requests
import csv
import asyncio
import time
from pyrogram import Client
from keep_alive import keep_alive  # Ensure you have this module or replace it with a similar functionality
#from dotenv import load_dotenv

# Load environment variables from .env file
#load_dotenv()

keep_alive()  # Keeps the script running (useful for cloud deployment)

# Define your variables
api_id = os.getenv('ApiId')  # Your API ID from Telegram
api_hash = os.getenv('ApiHash')  # Your API Hash from Telegram
session_url = os.getenv('SessionUrl')  # URL to download the session file
group_name = os.getenv('DestinationChatName')  # Group name for the destination group
user_id = os.getenv('UserIdForRegexMatch')  # Your user ID to match in messages
bot_username = os.getenv('BotToBeMonitoredNoAt')  # Bot's username without '@'
cancel_interval = int(os.getenv('CancelMessageInterval', 10))  # Interval to wait before sending a message, default to 10 seconds

# Download the session file
session_file = 'monitor.session'
try:
    session_response = requests.get(session_url)
    session_response.raise_for_status()  # Raise an exception for HTTP errors
    with open(session_file, 'wb') as f:
        f.write(session_response.content)  # Write the session file to disk
    print(f"Session file downloaded and saved as {session_file}")
except requests.exceptions.RequestException as e:
    print(f"Failed to download session file: {e}")
    exit(1)

# Ensure the session file exists
if not os.path.exists(session_file):
    print(f"Session file {session_file} does not exist.")
    exit(1)

session = "monitor"
# Initialize the Pyrogram Client with session file
app = Client(session, api_id=api_id, api_hash=api_hash)

# Function to get the group ID from the group name
async def get_group_id_by_name(group_name):
    for attempt in range(5):  # Retry up to 5 times
        try:
            async for dialog in app.get_dialogs():
                if dialog.chat.title == group_name:
                    return dialog.chat.id
            print(f"Group ID for {group_name} not found.")
            return None
        except Exception as e:
            print(f"Error retrieving group ID for {group_name}: {e}")
            if attempt < 4:  # Don't sleep on the last attempt
                time.sleep(3)  # Sleep for 3 seconds before retrying


# Function to save the line to CSV with UTF-8 encoding
def save_line_to_csv(line):
    with open('gids.csv', 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([line])
    extract_and_save_gid(line)

# Function to extract the desired part and store it in a new row
def extract_and_save_gid(line):
    try:
        # Extract the part after the 'l' in '/btsel'
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
        print("The line does not contain the expected format.")

# Function to clear the contents of gids.csv
def clear_gids_csv():
    with open('gids.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csvfile.truncate()  # Clear the file contents
    print("gids_extracted.csv formed => I am Ready!")

# Function to clear the contents of gids_extracted.csv
def clear_gids_extracted_csv():
    with open('gids_extracted.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csvfile.truncate()  # Clear the file contents
    print("gids_extracted.csv deployed => Here goes Nothing!")

# Function to send a custom message after a specific interval
async def send_custom_message(extracted_part):
    await asyncio.sleep(cancel_interval)  # Wait for the specified interval
    custom_message = f"/cancel{extracted_part}@{bot_username}"
    group_id = await get_group_id_by_name(group_name)
    if group_id:
        await app.send_message(group_id, custom_message)
    clear_gids_extracted_csv()
    print(f"Sent message: {custom_message}")

# Function to handle new messages
@app.on_message()
async def handler(client, message):
    # Get the group ID using the group name
    group_id = await get_group_id_by_name(group_name)
    if not group_id:
        print(f"Could not retrieve group ID for {group_name}.")
        return

    if message.chat.id == group_id:
        message_text = message.text
        # Split the message into lines
        lines = message_text.split('\n')

        # Iterate over the lines
        for i, line in enumerate(lines):
            if f"ID: {user_id}" in line:
                if i + 1 < len(lines):  # Ensure there is a line for the GID
                    gid_line = lines[i + 1]
                    save_line_to_csv(gid_line)

# Start the client
async def main():
    await app.start()
    print("Listening for messages in the group...")
    await asyncio.Event().wait()  # Keeps the client running

if __name__ == "__main__":
    loop = asyncio.get_event_loop_policy().get_event_loop()
    loop.run_until_complete(main())
