import datetime
import os

import emails
import pandas as pd
from airflow.utils.email import send_email
from utils.config import STAGE_NAMESPACE
from utils.teams_robot import error_message

def notify_email(contextDict, **kwargs):
    # Prepare the email
    message = emails.html(
        html="""
            Olá consagrado! <br>
            <br>
            Houve um erro na task {}.<br>
            <br>
            Descrição do erro: {}. <br>
        
            Até logo,<br>
            Airflow. <br>
            """.format(contextDict['task_instance_key_str'], contextDict['exception']),
        subject="Airflow alert: {} Failed".format(contextDict['dag']),
        mail_from="airflow@hdata.med.br",
    )

    error_message(title=STAGE_NAMESPACE,
                                      mentions=['lucas.freire@hdata.med.br'],
                                      message=['Falha no upsert_evolucao_paciente',
                                               contextDict['task_instance_key_str'],
                                               contextDict['exception'],
                                               contextDict['dag']],
                                      type='Stage')

    # Send the email
    r = message.send(
        to='raphael.queiroz@eximio.med.br',
        smtp={
            "host": "email-smtp.us-east-2.amazonaws.com",
            "port": 587,
            "timeout": 5,
            "user": os.environ['AWS_SMTP_USERNAME'],
            "password": os.environ['AWS_SMTP_PASSWORD'],
            "tls": True,
        },
    )

    # Check if the email was properly sent
    assert r.status_code == 250