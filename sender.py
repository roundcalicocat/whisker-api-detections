import smtplib
from email.mime.text import MIMEText
from config import SETTINGS

# based on https://mailtrap.io/blog/python-send-email-gmail/

#TODO: everything

subject = "Email Subject"
body = "This is the body of the text message"
sender = "sender@gmail.com"
recipient = SETTINGS["email"]["recipient"]
password = "password"


def send_email(subject, body, sender, recipients, password):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
       smtp_server.login(sender, password)
       smtp_server.sendmail(sender, recipients, msg.as_string())
    print("Message sent!")


send_email(subject, body, sender, recipients, password)