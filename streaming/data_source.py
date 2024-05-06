import os
import socket
import time

import requests


def get_info(lang):
    token = os.getenv('TOKEN')
    print(f"token {token}")
    url = f'https://api.github.com/search/repositories?q=+language:{lang}&sort=updated&order=desc&per_page=50'
    res = requests.get(url, headers={"Authorization": token})
    if res.status_code != 200:
        print(f"code {res.status_code}, err {res.content.decode('utf8')}")
        return ""
    return res.content


LANGUAGES = {"Python", "Java", "JavaScript"}


def main():
    TCP_IP = "0.0.0.0"
    TCP_PORT = 9999
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print("Waiting for TCP connection...")
    # if the connection is accepted, proceed
    conn, addr = s.accept()
    print("Connected... Starting sending data.")
    while True:
        try:
            for lang in LANGUAGES:
                info = get_info(lang)
                if info:
                    print(info)
                    conn.send(info + "\n".encode('utf8'))
            time.sleep(15)
        except KeyboardInterrupt:
            s.shutdown(socket.SHUT_RD)


if __name__ == "__main__":
    main()
