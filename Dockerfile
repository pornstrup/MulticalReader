FROM python:3-alpine

ADD requirements.txt /app/
WORKDIR /app
RUN pip3 install -r requirements.txt &&\
    rm requirements.txt

ADD /src/ /app

CMD  python3 /app/main.py