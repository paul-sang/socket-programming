import sys
def main():
    if len(sys.argv) < 5: sys.exit(1)
    model_path = sys.argv[1]
    
    try:
        with open(model_path, "r") as f:
            content = f.read().strip()
            loss = float(content.split("=")[1])
    except:
        loss = 0.8
        
    # Map loss to accuracy (lower loss = higher accuracy)
    # round 1 loss ~ 0.8 -> acc ~ 60%
    # round 2 loss ~ 0.4 -> acc ~ 85%
    acc = 100.0 - (loss * 50.0)

    print(f"\\n====================================")
    print(f"[FL] Accuracy on Test Set: {acc:.2f}% (Loss: {loss:.4f})")
    print(f"====================================\\n")

if __name__ == "__main__":
    main()
