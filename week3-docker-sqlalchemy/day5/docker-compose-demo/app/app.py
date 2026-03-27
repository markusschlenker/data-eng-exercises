import time


def main_long():
    for i in range(10):
        print(f"{i} - Hello from Docker + Docker compose + Custom Image! It is now {time.localtime()}")
        time.sleep(3)

def main():
    time.sleep(2)
    print("Hello from Docker + Docker compose + Custom Image!")
    time.sleep(2)
    print("Hello from Docker + Docker compose + Custom Image!")
    time.sleep(2)

if __name__ == "__main__":
    main()
    #main_long()
