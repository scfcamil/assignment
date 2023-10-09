FROM python:3.8-slim-buster
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./
COPY employee-details.csv ./

RUN pip install -r requirements.txt

COPY main.py ./

CMD [ "python3.8", "./main.py"]