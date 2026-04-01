from pathlib import Path

def main():
    print("Hello from Docker + Docker compose + Custom Image!")
    print(f"{Path.cwd()=}")

    with open("data/northwind_UTF8.csv", "r") as file:
        data = file.readlines()
        print("Data from northwind_UTF8.csv:")
        print(data[:3])  # print first 3 lines for brevity
    
if __name__ == "__main__":
    main()
