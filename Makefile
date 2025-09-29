create_network:
	docker network create my_network

server_image:
	docker build -f Dockerfile_server -t server_image .

lb_image:
	docker build -f Dockerfile_lb -t lb_image .

client_image:
	docker build -f Dockerfile_client -t client_image .

run_lb_database:
	docker run --name lb_database --hostname lb_database --network my_network -e MYSQL_ROOT_PASSWORD=password -d mysql:latest

run_lb_script:
	docker run --privileged -dit --name load_balancer --hostname load_balancer -v /var/run/docker.sock:/var/run/docker.sock --network my_network lb_image

clear_lb_database:
	docker exec -it lb_database mysql -uroot -ppassword -e "DROP DATABASE IF EXISTS Metadata;" 

run_client:
	docker run -dit --name client --hostname client --network my_network client_image
	docker exec -it client bash

run_server:
	docker run -dit --name server --hostname server --network my_network server_image

read_lb_logs:
	docker cp load_balancer:/usr/src/app/lb_logs ./lb_logs
	xdg-open lb_logs

read_server_logs:
	docker cp $(sname):/usr/src/app/server_logs ./$(sname)_logs
	xdg-open $(sname)_logs

rm_server_containers:
	docker rm server0 server1 server2 server3 server4 server5 server6 server7 server8 server9 server10 --force

rm_lb_containers:
	docker rm load_balancer lb_database --force

rm_all_containers:
	docker rm load_balancer lb_database server0 server1 server2 server3 server4 server5 server6 server7 server8 server9 server10 --force

dummy_image:
	docker build -f Dockerfile_dummy -t dummy_image .


run_dummy:
	docker run -dit --name client --hostname client --network my_network dummy_image
	
run_dummy_lb:
	docker run --privileged -dit --name load_balancer --hostname load_balancer -v /var/run/docker.sock:/var/run/docker.sock --network my_network dummy_image 
