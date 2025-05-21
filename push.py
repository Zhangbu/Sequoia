# -*- encoding: UTF-8 -*-
import sys
import time
import logging
import settings # Import the settings module
import smtplib
from email.mime.text import MIMEText
from email.message import EmailMessage
from datetime import datetime, timedelta, date
from wxpusher import WxPusher


nowtime = datetime.now()

def wxpush(msg):
    # Get the configuration using settings.get_config()
    config = settings.get_config()
    if config['push']['enable']:
        response = WxPusher.send_message(msg, uids=[config['push']['wxpusher_uid']],
                                         token=config['push']['wxpusher_token'])
        print(response)
    logging.info(msg)

def mail(message):
    logging.info("Sending email")
    try:
        # Get the configuration using settings.get_config()
        config = settings.get_config()
        smtp_server = config['mail']['smtp_server']
        from_addr = config['mail']['from_addr']
        password = config['mail']['password']
        to_addr = config['mail']['to_addr']
        conn = smtplib.SMTP_SSL(smtp_server, 465)
        conn.set_debuglevel(1)
        conn.login(from_addr, password)
        msg = EmailMessage()
        msg.set_content(message, 'plain', 'utf-8')
        msg['Subject'] = f'每日推荐 - {nowtime.strftime("%Y-%m-%d")}'
        msg['From'] = 'Stock Bot'
        msg['To'] = 'Investor'
        conn.sendmail(from_addr, [to_addr], msg.as_string())
        conn.quit()
        logging.info("Email sent successfully")
        return True
    except Exception as e:
        logging.error("Email error: %s", e)
        return False

def push(message):
    logging.info("Initiating push process")
    wxpush(message)
    # Check if email pushing is enabled before attempting to send mail
    config = settings.get_config()
    if config['mail']['enable']: # Assuming you have an 'enable' flag for mail in your config
        for attempt in range(10):
            if mail(message):
                logging.info("Email process completed")
                break
            logging.warning("Email attempt %d failed, retrying...", attempt + 1)
            time.sleep(1)
    else:
        logging.info("Email push is disabled in configuration.")

def statistics(msg=None):
    push(msg)

def strategy(msg=None):
    if msg is None or not msg:
        msg = '今日没有符合条件的股票'
    push(msg)

# # push.py
# import requests
# import json
# import logging
# import settings # Import settings to get push config

# logger = logging.getLogger(__name__)

# def strategy(message):
#     """Sends a strategy message using WXpusher."""
#     try:
#         push_config = settings.get_config().get('push', {})
#         enable_push = push_config.get('enable', False)
#         wxpusher_uid = push_config.get('wxpusher_uid', '')
#         wxpusher_token = push_config.get('wxpusher_token', '')

#         if not enable_push:
#             logger.info("WXpusher is disabled in settings.", extra={'stock': 'NONE', 'strategy': '推送'})
#             return

#         if not wxpusher_uid or not wxpusher_token:
#             logger.warning("WXpusher UID or Token is not configured. Cannot send message.", extra={'stock': 'NONE', 'strategy': '推送'})
#             return

#         url = "http://wxpusher.zjiecode.com/api/send/message"
#         headers = {'Content-Type': 'application/json'}
        
#         payload = {
#             "appToken": wxpusher_token,
#             "content": message,
#             "summary": "Stock Strategy Notification", # Optional message summary
#             "contentType": 1, # 1 for plain text
#             "uids": [wxpusher_uid]
#         }

#         response = requests.post(url, headers=headers, data=json.dumps(payload))
#         response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        
#         result = response.json()
#         if result.get('code') == 1000:
#             logger.info("WXpusher message sent successfully.", extra={'stock': 'NONE', 'strategy': '推送'})
#         else:
#             logger.error(f"Failed to send WXpusher message: {result.get('msg')}", extra={'stock': 'NONE', 'strategy': '推送'})

#     except requests.exceptions.RequestException as e:
#         logger.error(f"WXpusher network error: {e}", extra={'stock': 'NONE', 'strategy': '推送'})
#     except Exception as e:
#         logger.error(f"Failed to send WXpusher message due to an unexpected error: {e}", extra={'stock': 'NONE', 'strategy': '推送'})

# def send_mail(subject, content):
#     """Sends an email notification."""
#     try:
#         mail_config = settings.get_config().get('mail', {})
#         enable_mail = mail_config.get('enable', False)
#         smtp_server = mail_config.get('smtp_server', '')
#         smtp_port = mail_config.get('smtp_port', 465)
#         from_addr = mail_config.get('from_addr', '')
#         password = mail_config.get('password', '')
#         to_addr = mail_config.get('to_addr', '')

#         if not enable_mail:
#             logger.info("Email push is disabled in settings.", extra={'stock': 'NONE', 'strategy': '邮件'})
#             return

#         if not all([smtp_server, from_addr, password, to_addr]):
#             logger.warning("Email configuration incomplete. Cannot send mail.", extra={'stock': 'NONE', 'strategy': '邮件'})
#             return

#         import smtplib
#         from email.mime.text import MIMEText

#         msg = MIMEText(content, 'plain', 'utf-8')
#         msg['From'] = from_addr
#         msg['To'] = to_addr
#         msg['Subject'] = subject

#         server = smtplib.SMTP_SSL(smtp_server, smtp_port)
#         server.login(from_addr, password)
#         server.sendmail(from_addr, to_addr, msg.as_string())
#         server.quit()
#         logger.info("Email sent successfully.", extra={'stock': 'NONE', 'strategy': '邮件'})

#     except Exception as e:
#         logger.error(f"Failed to send email: {e}", extra={'stock': 'NONE', 'strategy': '邮件'})

# # Example usage (for testing push.py directly)
# if __name__ == '__main__':
#     # Initialize settings for standalone run
#     import sys
#     if not logging.getLogger().handlers:
#         logging.basicConfig(
#             level=logging.DEBUG,
#             format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
#             handlers=[logging.StreamHandler(sys.stdout)]
#         )
    
#     # Create a dummy config for testing if settings.py is not fully set up
#     class DummySettings:
#         def get_config(self):
#             return {
#                 'push': {
#                     'enable': True,
#                     'wxpusher_uid': 'your_wxpusher_uid_for_testing', # Replace
#                     'wxpusher_token': 'your_wxpusher_token_for_testing' # Replace
#                 },
#                 'mail': {
#                     'enable': False, # Set to True to test mail
#                     'smtp_server': 'smtp.163.com',
#                     'from_addr': 'your_email@163.com',
#                     'smtp_port': 465,
#                     'password': 'YOUR_EMAIL_APP_PASSWORD',
#                     'to_addr': 'recipient_email@example.com'
#                 }
#             }
    
#     settings = DummySettings() # Overwrite settings with dummy for testing
    
#     logger.info("Testing WXpusher...")
#     strategy("这是一个来自股票策略助手的测试消息！")

#     # logger.info("Testing Email...")
#     # send_mail("股票策略测试邮件", "这是邮件测试内容。")