#!/bin/bash

# 创建一个新的 tmux 会话
tmux new-session -d -s chord

# 基础端口号
BASE_CHORD_PORT=8000
BASE_SSH_PORT=2000

# 创建 2x4 布局
# 垂直分割
tmux split-window -v

# 上半部分水平分割
tmux select-pane -t 0
tmux split-window -h
tmux select-pane -t 0
tmux split-window -h
tmux select-pane -t 0
tmux split-window -h

# 下半部分水平分割
tmux select-pane -t 4
tmux split-window -h
tmux select-pane -t 4
tmux split-window -h
tmux select-pane -t 4
tmux split-window -h

# 为每个窗格启动一个节点
# 第一个节点 (创建新环)
tmux select-pane -t 0
tmux send-keys "./ChordClient -a 127.0.0.1 -p $BASE_CHORD_PORT -sp $BASE_SSH_PORT -ts 3000 -tff 1000 -tcp 3000 -tb 1000 -r 4" C-m

# 等待第一个节点启动
sleep 2

# 其余节点 (加入已有的环)
for i in {1..7}
do
    tmux select-pane -t $i
    CHORD_PORT=$((BASE_CHORD_PORT + i))
    SSH_PORT=$((BASE_SSH_PORT + i))
    tmux send-keys "./ChordClient -a 127.0.0.1 -p $CHORD_PORT -sp $SSH_PORT -ts 3000 -tff 1000 -tcp 3000 -tb 1000 -r 4 -ja 127.0.0.1 -jp $BASE_CHORD_PORT" C-m
    sleep 2
done

# 进入 tmux 会话
tmux attach-session -t chord