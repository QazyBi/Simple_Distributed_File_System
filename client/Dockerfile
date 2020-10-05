FROM python
WORKDIR /client
COPY . /client
RUN pip install -r requirements.txt
EXPOSE 8080
CMD ["python", "client.py"]
