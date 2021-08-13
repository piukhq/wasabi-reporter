FROM ghcr.io/binkhq/python:3.9

WORKDIR /app
ADD . .

RUN pip install --no-cache-dir pipenv && \
    pipenv install --system --deploy --ignore-pipfile

CMD ["python", "main.py"]
