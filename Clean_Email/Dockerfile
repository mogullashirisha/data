FROM python:3.6

# set display port to avoid crash
ENV DISPLAY=:99

# Install requirements first so this step is cached by Docker
COPY /requirements.txt /home/ubuntu/data/clean_email/requirements.txt
WORKDIR /home/ubuntu/data/clean_email/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# copy code
COPY /clean_email.py  /home/ubuntu/data/clean_email/clean_email.py

ENTRYPOINT ["python","clean_email.py"]
