1. This project contains server & proxy, both of them can be packed as docker images
through the dockerfile. 
2. To build server docker image:
    cd server && docker build . -t server:v1
3. To build proxy docker image:
    cd proxy && docker build . -t proxy:v1
4. start dockter containter and test: see ./test/test 
