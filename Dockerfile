FROM public.ecr.aws/docker/library/python:3.9-slim-buster


WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

EXPOSE 8081
COPY . /app

# Run the web service on container startup.
CMD ["python", "app.py"]

