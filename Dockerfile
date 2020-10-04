FROM python:3.8.6-buster

WORKDIR /nameserver

COPY . /nameserver

RUN pip install --trusted-host pypi.python.org -r requirements.txt

EXPOSE 8080

CMD ["python", "nameserver.py"]