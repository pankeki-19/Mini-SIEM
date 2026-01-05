FROM python:3.11-slim
WORKDIR /app
COPY ../common/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY ../common/ /app/common
COPY . /app
CMD ["python", "main.py"]