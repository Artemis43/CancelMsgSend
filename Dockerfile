FROM python:3.11-alpine3.20
WORKDIR /sbot
COPY . /sbot/
RUN pip install -r requirements.txt
EXPOSE 4343
CMD python main.py
