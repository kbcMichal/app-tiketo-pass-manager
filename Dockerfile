FROM python:3.12-slim

COPY . /code/
WORKDIR /code/

RUN pip install --no-cache-dir -e .

WORKDIR /data/
CMD ["python", "-u", "/code/src/component.py"]
