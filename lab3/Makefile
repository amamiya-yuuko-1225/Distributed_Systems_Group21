build_create_name="chord_create"
build_join_name="chord_join"
dockerfile_create="create.Dockerfile"
dockerfile_join="join.Dockerfile"

# # Command for creating access keys
create_sftp_keys:
	ssh-keygen -t rsa -b 4096 -f ${PWD}/keys/id_rsa

create_keys:
	openssl genrsa -out ./keys/encryption-priv.pem
	openssl rsa -in ./keys/encryption-priv.pem -pubout -out ./keys/encryption-pub.pem

# command for creating SFTP server
sftp_server_create:
	docker run --rm -d -p $(p):22 -v ${PWD}/keys/id_rsa.pub:/home/foo/.ssh/keys/id_rsa.pub:ro -v ${PWD}/keys/id_rsa:/etc/ssh/ssh_host_rsa_key:ro atmoz/sftp foo::1001::resources

chord_image_build:
	docker build -t $(build_create_name) -f ${dockerfile_create} .
	docker build -t $(build_join_name) -f ${dockerfile_join} .

# command for creating docker image of initial node of chord ring
chord_start:
	$(MAKE) sftp_server_create p=$(sp)
	docker run -dit --rm --publish $(p):$(p) -e ADDRESS=$(a) -e PORT=$(p) -e SSH_PORT=$(sp) $(build_create_name)

# command for running docker image for joining chord ring
chord_join:
	$(MAKE) sftp_server_create p=$(sp)
	docker run -it --rm --publish $(p):$(p)  -e ADDRESS=$(a) -e PORT=$(p) -e SSH_PORT=$(sp) -e JOIN_ADDRESS=$(ja) -e JOIN_PORT=$(jp) $(build_join_name)

# docker ps -a
# docker kill $(docker ps -q)


# # docker rmi chord_join
# # docker rmi chord_create

# # make chord_start a=34.193.1.217 p=8080 sp=9080
# # make chord_join a=52.200.235.170 p=8081 sp=9081 ja=34.193.1.217 jp=8080

# # make sftp_server_create
# # make chord_image_build