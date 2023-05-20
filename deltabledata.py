import pyodbc
import datetime
import yaml
import os, sys
import traceback
import logging
from django.core.mail import send_mail
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
print(BASE_DIR, "base dir")
import os, sys
import argparse
import logging
import logging.config
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


args = sys.argv[1:]
parser = argparse.ArgumentParser(
    prog="DELETEDATA", description="Delete Data")

parser.add_argument("-c", "--config", required=False,
                     help="Input YAML Configuration file (Required)")

options = parser.parse_args()


try:
    # load config file to collect application settings
    if options.config is None:  
        config_file_name= r'D:\\ETL_Task_CGS\\Delete_Data\\config.yaml'      
    else:
        config_file_name=options.config

    with open(config_file_name, 'r') as file:
        config = yaml.safe_load(file.read())
except Exception as e:
    raise e
    sys.exit()
finally:
    pass



def setupLogger(config):
    dir_name = os.path.dirname(os.path.realpath(__file__))
    logger_config_file = config['file']['log_config']
    version = "1.0" 
    try:
        # read in logger configuration and setup logger
        with open(logger_config_file, 'r') as f:
            logger_config = yaml.safe_load(f.read())
            logging.config.dictConfig(logger_config)

        logger = logging.getLogger('AppLogger')
    except Exception as e:
        logger.error(e)
    finally:
        logger.info(
            "#########################################################################################")
        logger.info("CGS-CIMB LOGGER - VERSION: "+version)
        logger.info("Directory Path: "+dir_name)
        logger.info("Logger Config File:"+logger_config_file)
        logger.info(logger_config)
        logger.info(config)
        logger.info("User Account:"+os.environ['userdomain']+"\\"+os.getlogin())
        logger.info(
            "#########################################################################################")

    return logger




try: 

    app_logger = setupLogger(config)
    app_logger.info("Loading configuation details from: "+config_file_name)
except Exception as e:
    app_logger.error(e)
    
finally:
    pass


DEFAULT_FROM_EMAIL = 'sg.rpa@cgs-cimb.com'
#DEFAULT_TO_EMAIL = ["ramireddy.b@cgs-cimb.com"]
DEFAULT_TO_EMAIL = ["jayshri.r@rosemallowtech.com"]

STATIC_URL = '/static/'
#EMAIL_BACKEND = ''
EMAIL_HOST = "10.3.10.42"
EMAIL_PORT = "25"
#EMAIL_HOST_USER = ''
#EMAIL_HOST_PASSWORD = ''

class ReadConfig:
    lst_globalerrors=[]
    filename=""
    logfile=""
    def __init__(self):
        with open(os.getcwd()+"\\config.yaml",'r') as file:
            self.ConfigDocumentContent=yaml.full_load(file)
    def getConfigvalue(self,key):
        return self.ConfigDocumentContent.get(key)
    
try:
    _readconfig=ReadConfig() 
    Driver=_readconfig.ConfigDocumentContent["DBConfig"]["sqldrivername"]
    Server=_readconfig.ConfigDocumentContent["DBConfig"]["sqlservername"]
    Database=_readconfig.ConfigDocumentContent["DBConfig"]["sqldbname"]
    UID=_readconfig.ConfigDocumentContent["DBConfig"]["sqluserid"]
    PWD=_readconfig.ConfigDocumentContent["DBConfig"]["sqlpwd"]
    days_diff_older_data = datetime.date.today() - datetime.timedelta(days=_readconfig.ConfigDocumentContent["days_diff"])
except Exception as e:
        raise Exception(e)


app_logger.info("deleting data older than : " + str(days_diff_older_data))

def deleteDatafromDB(days):
    try:
        days = days
        _readconfig=ReadConfig() 
        query=_readconfig.ConfigDocumentContent["Queries"]["delete_data"] 
        with pyodbc.connect('Driver='+Driver+';'\
                    'Server='+Server+';'\
                    'Database='+Database+';'\
                    'UID='+UID+';'\
                    'PWD='+PWD+';'\
                    'Trusted_Connection=No;') as conn:
            with conn.cursor() as c:
                app_logger.info("Query : " + str(query))
                rows=c.execute(query,days)
    except Exception as e:
        sendmailtask("An error occured in delete older data script : " + str(e))
        app_logger.error("Error in delete data script : " + str(e))
        raise logging.exception(e)


def sendmailtask(strmsg):
    host = _readconfig.ConfigDocumentContent['EmailConfig']['SMTP']
    TO = _readconfig.ConfigDocumentContent['EmailConfig']['Emailto']
    CC = _readconfig.ConfigDocumentContent['EmailConfig']['CC']
    Subject = _readconfig.ConfigDocumentContent['EmailConfig']['Subject']
    LogErrorFile = _readconfig.ConfigDocumentContent['file']['log_error_file']
    
    FROM = "sg.rpa@cgs-cimb.com"
    msg = MIMEMultipart()
    msg['From'] = FROM
    msg['To'] = ', '.join(TO)
    msg['Subject'] = Subject
    body = "Error while Running  delete prev data function please check log {0} \n {1}".format(LogErrorFile,strmsg)    
    msg.attach(MIMEText(body, 'plain'))
    if os.path.exists(LogErrorFile):
        msg.attach(MIMEText("Labour"))
        attachment = MIMEBase('application', 'octet-stream')
        with open(LogErrorFile, 'rb') as fp:
            attachment.set_payload(fp.read())
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment',filename=os.path.basename(LogErrorFile))
        msg.attach(attachment)
    server = smtplib.SMTP(host)
    server.sendmail(FROM, TO, msg.as_string())
    server.quit()



if __name__ == "__main__":
    try:
        app_logger.info("delete older days data functionality started .")
        print("delete older days data functionality started .")
        deleteDatafromDB(days_diff_older_data)
        print("delete older days data functionality Completed Successfully.")
        app_logger.info("delete older days data functionality Completed Successfully.")
    except Exception as e:
        raise app_logger.exception("Error occured in the delete data script : " + str(e))
    
    