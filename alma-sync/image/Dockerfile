FROM python:3.6
COPY ./requirements.txt /app/
WORKDIR /app
RUN pip install -r requirements.txt
COPY ./*.py /app/
ENV PATH=$PATH:/app
ENV HOME=/home/userdir
CMD cadc-alma-sync.py
