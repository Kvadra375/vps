#!/bin/bash

# Получаем IP сервера (основной интерфейс)
SERVER_IP=$(ip -4 addr show | grep -oP '(?<=inet\s)\d+\.\d+\.\d+\.\d+' | grep -v '127.0.0.1' | head -n 1)

# Создаём пользователя git
sudo adduser --disabled-password --gecos "" git

# Настраиваем SSH
sudo -u git mkdir -p /home/git/.ssh
sudo -u git touch /home/git/.ssh/authorized_keys
sudo chmod 700 /home/git/.ssh
sudo chmod 600 /home/git/.ssh/authorized_keys

# Устанавливаем git-shell
sudo chsh -s $(which git-shell) git
if ! grep -q "$(which git-shell)" /etc/shells; then
    sudo bash -c "echo $(which git-shell) >> /etc/shells"
fi

# Выводим строку для копирования
echo "SSH string for Git: git@$SERVER_IP"
echo "Now add your public key to /home/git/.ssh/authorized_keys"
