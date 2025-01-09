import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class MailClient:
    def __init__(self, address: str, port: int, login_name: str, password: str):
        self.__address = address
        self.__port = port
        self.__login_name = login_name
        self.__password = password

    def send(self, to_addr: str, from_addr: str, header: str, text: str):
        server = smtplib.SMTP(self.__address, self.__port)
        server.starttls()
        server.login(self.__login_name, self.__password)
        message = MIMEMultipart()
        message["From"] = from_addr
        message["To"] = to_addr
        message["Subject"] = header
        message.attach(MIMEText(text, 'plain'))
        server.sendmail(from_addr, to_addr, message.as_string())
        server.quit()


