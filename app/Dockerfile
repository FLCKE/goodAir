FROM python:3.13
WORKDIR /app


COPY . .
RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y iputils-ping
CMD ["python","./main.py"]