## You must do this to keep Airflow container running
<ins>install cryptography</ins>
- pip install cryptography

<ins>generate FERNET Key</ins> 
- PS C:\Users\kdabc\pipeline_with_airflow> python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

<ins>Update docker-compose.yaml</ins>
- AIRFLOW__CORE__FERNET_KEY: "<Generated_Key>"

<ins>restart docker</ins>
docker-compose down 
docker-compose up -d
