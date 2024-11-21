FROM python:3.10-alpine

# copy entire directory into container
COPY . /app/dcm-job-processor
# copy accessories
COPY ./app.py /app/app.py

# set working directory
WORKDIR /app

# install/configure app ..
RUN pip install --upgrade \
    --extra-index-url https://zivgitlab.uni-muenster.de/api/v4/projects/9020/packages/pypi/simple \
    "dcm-job-processor/[cors]"
RUN rm -r dcm-job-processor/
ENV ALLOW_CORS=1

# .. and wsgi server (gunicorn)
RUN pip install gunicorn

# define startup
ENTRYPOINT [ "gunicorn" ]
CMD ["--bind", "0.0.0.0:8080", "app:app"]
