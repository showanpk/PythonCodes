from azure.communication.sms import SmsClient
import os

CONNECTION_STRING = os.getenv("AZURE_COMMUNICATION_CONNECTION_STRING", "")
FROM_SENDER = os.getenv("AZURE_SMS_FROM", "Sahelihub")
TO_PHONE = os.getenv("AZURE_SMS_TO", "")

if not CONNECTION_STRING:
    raise ValueError("Set AZURE_COMMUNICATION_CONNECTION_STRING in environment variables.")
if not TO_PHONE:
    raise ValueError("Set AZURE_SMS_TO in environment variables.")

try:
    print("Connection string configured:", bool(CONNECTION_STRING))
    print("From:", FROM_SENDER)
    print("To:", TO_PHONE)

    sms_client = SmsClient.from_connection_string(CONNECTION_STRING)

    responses = sms_client.send(
        from_=FROM_SENDER,
        to=[TO_PHONE],
        message="Test SMS from Sahelihub."
    )

    for response in responses:
        print("Successful:", response.successful)
        print("Message ID:", response.message_id)
        print("To:", response.to)
        if not response.successful:
            print("Error:", response.error_message)

except Exception as ex:
    print("Exception occurred:")
    print(ex)