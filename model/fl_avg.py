import sys
def main():
    if len(sys.argv) < 3: sys.exit(1)
    out_global = sys.argv[1]
    w_files = sys.argv[2:]
    
    total_loss = 0.0
    for wf in w_files:
        try:
            with open(wf, "r") as f:
                content = f.read().strip()
                loss = float(content.split("=")[1])
                total_loss += loss
        except:
            total_loss += 0.8 # fallback

    avg_loss = total_loss / len(w_files)
    with open(out_global, "w") as f:
        f.write(f"loss={avg_loss}\n")

if __name__ == "__main__":
    main()
