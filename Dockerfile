# FROM amazon/aws-eb-python:3.7.10-buster
FROM python:3.7.10-buster

# COPY requirements.txt .

# RUN pip install -r requirements.txt

# COPY application.py .
# COPY to_csv_on_s3.py .

# CMD ["python", "application.py"]

# RUN docker run -p 8080:8080 -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY jpx-future-app
# The port number must be the same as the one in the application.py


# Follow https://www.youtube.com/watch?v=yjb5kfRUw0A
# Basically, run
# 1. aws ecr get-login-password --region eu-west-3 | docker login --username AWS --password-stdin 318739777477.dkr.ecr.eu-west-3.amazonaws.com
# 2. docker build -t jpx-future-app .
# 3. docker tag jpx-future-app:latest 318739777477.dkr.ecr.eu-west-3.amazonaws.com/jpx-future-app:latest
# 4. docker push 318739777477.dkr.ecr.eu-west-3.amazonaws.com/jpx-future-app:latest


# See https://towardsdatascience.com/how-to-use-docker-to-deploy-a-dashboard-app-on-aws-8df5fb322708
# 1-dep. docker tag jpx_future_app 318739777477.dkr.ecr.eu-central-1.amazonaws.com/jpx-future-app
# 2-dep. aws ecr get-login-password | docker login --username AWS --password-stdin 318739777477.dkr.ecr.eu-central-1.amazonaws.com/jpx-future-app
# 3-dep. docker push 318739777477.dkr.ecr.eu-central-1.amazonaws.com/jpx-future-app


# FROM continuumio/miniconda3
COPY requirements.txt /tmp/
COPY ./app /app
WORKDIR "/app"
RUN pip install -r /tmp/requirements.txt
# ENTRYPOINT [ "python3" ]
# CMD [ "application.py" ]
CMD ["python", "application.py"]