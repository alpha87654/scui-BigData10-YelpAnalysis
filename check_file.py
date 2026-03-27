import os

print("Current working folder:")
print(os.getcwd())

print("\nFirst 3 lines of dataset/sf-fire-calls.txt:\n")

with open("dataset/sf-fire-calls.txt", "r", encoding="utf-8", errors="ignore") as f:
    for _ in range(3):
        print(f.readline().strip())