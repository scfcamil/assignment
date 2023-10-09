# Data engineering assignment

### Executing the etl process as a container

- create a docker network called 'etl_mongo_connector' if it does not exist
    ```
    docker network inspect etl_mongo_connector ||
    docker network create --driver bridge etl_mongo_connector
    ```
- build a Docker image 'etl_main' for the ETL application by running
    
    ```
    cd ./main   
    docker build . -t etl_main
  ```
- run docker-compose up


### Running local unit tests 

- run  ```pip install pytest``` 
- run  ```python3 -m pytest -v``` from ```./assignment``` directory

