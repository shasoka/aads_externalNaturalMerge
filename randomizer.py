import random


for i in range(5):
    buf = '\n'.join(str(random.randint(-1000, 1000)) for j in range(7))
    with open(f"{i}.txt", "w") as f:
        f.write(buf)
