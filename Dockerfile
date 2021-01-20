FROM binkhq/python:3.8

WORKDIR /app
ADD . .

RUN pip install --no-cache-dir pipenv && \
    pipenv install --system --deploy --ignore-pipfile

CMD ["python", "main.py"]
