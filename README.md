<!--
 * @Author: amamiya-yuuko-1225 1913250675@qq.com
 * @Date: 2024-12-02 21:53:02
 * @LastEditors: amamiya-yuuko-1225 1913250675@qq.com
 * @Description: 
-->
# DS LAB2 GROUP21

All requirements are implemented.

The basic part impl. and bonus impl are different.

For the basic part, all files are on local machine, and the master waits for 10s to
check worker failure and reallocate tasks accordingly.

For the bonus part, source files are on the master, and map/reduce output files are 
on the machine who produced them. Moreover, the master keeps pining workers (recieving heart beats) to check failure. Master failure can be detected by workers.