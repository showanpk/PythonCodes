import os
import time
from azure.communication.sms import SmsClient


connection_string = os.getenv("COMMUNICATION_SERVICES_CONNECTION_STRING")

if not connection_string:
    raise ValueError(
        "Connection string is missing. "
        "Set the COMMUNICATION_SERVICES_CONNECTION_STRING environment variable."
    )

sms_client = SmsClient.from_connection_string(connection_string)

sender_id = "Sahelihub"

numbers = [
    "+41799264738",    # from 0041799264738
    "+817044253083",   # Japan
    "+447841642966",   # UK
    "+447834368636",   # UK
    "+4790594657",     # from 004790594657
    "+61404499316",    # Australia
    "+61411441914",    # Australia
    "+61403741113",    # Australia
]

# numbers = [
#     "+447400006716",   # UK
# ]
message = """Hello,

Just to confirm you have a seat booked for the sunset boating session at 6pm.
Please note the paddle boat will be outside the ICC entrance, canalside.
Meeting point photo: https://shorturl.at/GWhxY"""

for number in numbers:
    try:
        print(f"Sending SMS to {number}...")

        sms_responses = sms_client.send(
            from_=sender_id,
            to=number,
            message=message,
            enable_delivery_report=True
        )

        for response in sms_responses:
            if response.successful:
                print(f"SUCCESS: Message sent to {response.to}")
                print(f"Message ID: {response.message_id}")
            else:
                print(f"FAILED: Message not sent to {response.to}")
                print(f"Error code: {response.error_message}")

        # Small delay to avoid sending too fast
        time.sleep(1)

    except Exception as ex:
        print(f"ERROR sending to {number}")
        print(ex)

print("Finished sending SMS messages.")
