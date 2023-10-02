FROM apache/airflow:2.7.1
COPY ./requirements_4_image.txt ./requirements_4_image.txt
RUN pip install -r requirements_4_image.txt