<!--
 * @Author: amamiya-yuuko-1225 1913250675@qq.com
 * @Date: 2024-12-17 11:01:06
 * @LastEditors: amamiya-yuuko-1225 1913250675@qq.com
 * @Description: 
-->
## lab3 group 21

All requirements are met, including chord protocol with r sized successor list and automatic file backup (in r successors), safe file storage (AES encrypted) and transfer (ssh/sftp)

Source code in "". "main.go" is for demo, in which ssh/sftp function is disabled (for conveniency of deployment)

## Command usage
instructions after running:
Lookup <filename> or l <filename>

StoreFile <filepath> [ssh] [encrypt] or s <filepath> [ssh] [encrypt] 
(use "s <filepath> f f" to disale encryption)

PrintState or p

## HOW TO START:
see deploy.txt

## BEFORE USING SSH/SFTP: generate keys:
1. mkdir keys
2. ssh-keygen -t rsa -f keys/id_rsa -N "" 
//generate SSH key pair

