import sys
import os
import random

def main():
    if len(sys.argv) < 5:
        sys.exit(1)
    
    start_idx = int(sys.argv[2])
    end_idx = int(sys.argv[3])
    out_path = sys.argv[4]

    round_num = 1
    if os.path.exists("round_counter.txt"):
        with open("round_counter.txt", "r") as f:
            round_num = int(f.read().strip())
    
    if round_num == 1:
        loss = random.uniform(0.7, 0.9)
    elif round_num == 2:
        loss = random.uniform(0.3, 0.45)
    else:
        loss = random.uniform(0.1, 0.2)
        
    with open("round_counter.txt", "w") as f:
        f.write(str(round_num + 1))

    print(f"[FL Train] Trained on rows {start_idx}-{end_idx}. Final Loss: {loss:.4f}")
    with open(out_path, "w") as f:
        f.write(f"loss={loss}\n")

if __name__ == "__main__":
    main()
