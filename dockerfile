FROM python:3.11-slim
WORKDIR /app
COPY hello_world.py /app/
CMD ["python", "/app/hello_world.py"]