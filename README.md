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
6. ```bash
docker pull mongo
```
7. Create a docker container with a Mongo database. Change "mongo-on-docker" with the container name, mongoadmin with the root username, and "root" with the root password. 
```bash
docker run -d --network localhost  --name mongo-on-docker  -p 27888:27017 -e MONGO_INITDB_ROOT_USERNAME=mongoadmin -e MONGO_INITDB_ROOT_PASSWORD=root mongo
```
7. Go the the docker bash. Write "docker ps" in the terminal to display the name. 
```bash
docker exec -it [container_name] bash

mkdir -p /data/db

mongod --repair
```
8. Go into mysql
```bash
mongo admin -u USER_PREVIOUSLY_DEFINED
use test_db
```
9. An password input is prompted. Write the password used in step 6.
10. Create a user with all read/write rights:
```bash
db.createUser({user: "username",pwd: "password",roles: ["readWrite"]});
```
11. Give that user all rights:
```bash
GRANT ALL PRIVILEGES ON *.* TO ‘YOUR_USERNAME_HERE’@’%’ WITH GRANT OPTION;
```

Yes, write the username using quotas ‘’. Here is an example for username=testuser and password=test123:
```bash
CREATE USER ‘testuser’@’%’ IDENTIFIED BY ‘test123’;
GRANT ALL PRIVILEGES ON *.* TO ‘testuser’@’%’ WITH GRANT OPTION;
```
12. Flush the privileges
```bash
FLUSH PRIVILEGES;
```
13. Check if the user is created:
```bash
SELECT User FROM mysql.user;
```
14.Create a database:
```bash
CREATE DATABASE test_db;
```
15. Check if it is created:
```bash
SHOW DATABASES;
```
16. Write "exit" twice or open another terminal.
17. Restart the docker container:
```bash
docker restart [container id]
```
You can fetch the container id by typing:
```bash
docker ps
```
