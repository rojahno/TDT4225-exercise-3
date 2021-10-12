# TDT4225-exercise-2

## Steps:
1. Clone the repository
2. Add the dataset to the files. The name of the dataset folder must be "dataset". The gitignore ignores this folder, and we wont have a large repository. 
3. Install requirements.
```python
pip install -r requirements.txt
```
4. Add an envirenment file(.env file) to store database password and username in your project.
 ```python
PASSWORD = [your password]
USERNAME = [your username]
```

5. Pull Mogno
```bash
docker pull mongo
```
6. Create a docker container with a Mongo database. Change "mongo-on-docker" with the container name, mongoadmin with the root username, and "root" with the root password. Remember to remove the brackets from username and password.
```bash
docker run -d --name mongo-on-docker  -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=[mongoadmin] -e MONGO_INITDB_ROOT_PASSWORD=[root] mongo --bind_ip_all
```
7. Go the the docker bash. Write "docker ps" in the terminal to display the name. 
```bash
docker exec -it mongo-on-docker bash
```
8. Go into mongo with auth rights
```bash
mongo admin -u USER_PREVIOUSLY_DEFINED
```
9. An password input is prompted. Write the password used in step 6.
10. Create a user with all read/write rights:
```bash
use test_db
db.createUser({user: "username",pwd: "password",roles: ["readWrite"]});
```
An erro with connecting with the database. Think the error is in the docker setup.
