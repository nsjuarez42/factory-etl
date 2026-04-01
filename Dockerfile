FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
RUN pip install python-dotenv
CMD ["python3","/app/src/processor.py"]


