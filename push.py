# -*- encoding: UTF-8 -*-
import sys
import time
import logging
import settings
import smtplib
from email.mime.text import MIMEText
from email.message import EmailMessage
from datetime import datetime, timedelta, date
from wxpusher import WxPusher


nowtime = datetime.now()

def wxpush(msg):
    if settings.config['push']['enable']:
        response = WxPusher.send_message(msg, uids=[settings.config['push']['wxpusher_uid']],
                                         token=settings.config['push']['wxpusher_token'])
        print(response)
    logging.info(msg)

def mail(message):
    logging.info("Sending email")
    try:
        smtp_server = settings.config['mail']['smtp_server']
        from_addr = settings.config['mail']['from_addr']
        password = settings.config['mail']['password']
        to_addr = settings.config['mail']['to_addr']
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
    logging.info("Initiating email process")
    wxpush(message)
    for attempt in range(10):
        if mail(message):
            logging.info("Email process completed")
            break
        logging.warning("Email attempt %d failed, retrying...", attempt + 1)
        time.sleep(1)    


def statistics(msg=None):
    push(msg)


def strategy(msg=None):
    if msg is None or not msg:
        msg = '今日没有符合条件的股票'
    push(msg)
